// A worker agent process. Usage:
//   bun run worker/index.ts worker w1 t1    (owns task t1 from attempt 1, exits when done)
//   bun run worker/index.ts standby s1      (waits for a Task actor to offer a lost task, then continues it)
import type { RelayAgentClient } from "@agent-relay/sdk"
import { actors } from "../generated/index.js"
import { actorId, agentName, correlationId, env, loadRun, relayKey, sleep, type RelayMeta } from "../shared/config"
import { joinAs, joinChannel, readMeta, relay, send } from "../shared/relay"
import { FACT_SHEETS } from "./data"
import { writeStep } from "./llm"

const [role, short, firstTask] = process.argv.slice(2) as ["worker" | "standby", string, string | undefined]
if (!role || !short || (role === "worker" && !firstTask)) {
    console.error("usage: worker/index.ts worker <name> <taskId> | standby <name>")
    process.exit(2)
}

const run = loadRun()
const me = agentName(run.runId, short)
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), me, ...a)

type Directive = { id: string; text: string; from: string; at: number }
type Ledger = { stepId: number; summary: string; worker: string }[]
type Draft = { stepId: number; worker: string; text: string } | null
type Current = { taskId: string; attempt: number; directives: Directive[] }
type ClaimInfo = { attempt: number; ledger: Ledger; directives: Directive[]; cutOff: Draft; nextStep: number }

let current: Current | null = null
// Set synchronously before the first await of a claim, so an actor offer, a poll and a
// retry arriving together can never start the same (or a second) task in this process.
let busy: symbol | null = null
const handled = new Set<string>()
let client: RelayAgentClient | null = null

// Demo fault: SIGUSR1 makes this agent hang. Its heartbeat keeps the lease alive, but it stops
// making progress, so only the actor's progress deadline can tell that it is stuck.
let hung = false
process.on("SIGUSR1", () => {
    hung = true
    log("SIGUSR1: hanging (heartbeat continues, no more progress)")
})

const FENCED = new Set(["stale-attempt", "wrong-owner"])
const DRAFT_EVERY_MS = 350

// Actor calls: 3 s timeout per attempt, retries at 0.5 s, 1 s, 2 s, then throw.
// A worker only gives up once the actor has been unreachable for longer than a lease,
// because only then can another agent own its task.
let lastOkAt = Date.now()
async function call<T>(fn: () => Promise<T>): Promise<T> {
    let error: unknown
    for (const delay of [0, 500, 1000, 2000]) {
        if (delay) await sleep(delay)
        try {
            const result = await Promise.race([fn(), sleep(3000).then(() => Promise.reject(new Error("actor call timeout")))])
            lastOkAt = Date.now()
            return result as T
        } catch (e) {
            error = e
        }
    }
    throw error
}

// Main-flow wrapper: keeps retrying through an actor outage shorter than a lease.
async function callOrExit<T>(fn: () => Promise<T>): Promise<T> {
    for (;;) {
        try {
            return await call(fn)
        } catch (e: any) {
            if (Date.now() - lastOkAt > env.leaseMs + 2000) {
                log(`actor unreachable for over a lease (${e?.message}), exiting`)
                process.exit(1)
            }
        }
    }
}

function fenced(taskId: string, reason: string) {
    log(`write fenced on ${taskId}: ${reason}; another agent owns this task now, exiting`)
    process.exit(0)
}

// Streams the text being written into the actor, at most every DRAFT_EVERY_MS, so the board
// shows it live and a successor can finish the sentence if this process dies.
function draftStreamer(taskId: string, attempt: number, stepId: number) {
    const task = actors.Task.get(actorId(run.runId, taskId))
    let latest = ""
    let sent = ""
    let inFlight = false
    const timer = setInterval(async () => {
        if (inFlight || latest === sent) return
        inFlight = true
        const text = latest
        try {
            const r = await call(() => task.writeDraft({ worker: me, attempt, stepId, text }))
            if (!r.ok && FENCED.has(r.reason)) fenced(taskId, r.reason)
            sent = text
        } catch {
            // The heartbeat decides whether the actor is really gone.
        } finally {
            inFlight = false
        }
    }, DRAFT_EVERY_MS)
    return { push: (text: string) => void (latest = text), stop: () => clearInterval(timer) }
}

async function runTask(taskId: string, info: ClaimInfo) {
    const { attempt } = info
    const task = actors.Task.get(actorId(run.runId, taskId))
    const sheet = FACT_SHEETS.find(s => s.id === taskId)!
    current = { taskId, attempt, directives: [...info.directives] }
    log(`running ${taskId} attempt ${attempt} from step ${info.nextStep}${info.cutOff ? ` (finishing ${info.cutOff.worker}'s cut-off step ${info.cutOff.stepId})` : ""}`)

    // The heartbeat has its own timer so a slow LLM call does not expire the lease.
    const beat = setInterval(async () => {
        try {
            const r = await call(() => task.heartbeat({ worker: me, attempt }))
            if (!r.ok && FENCED.has(r.reason)) fenced(taskId, r.reason)
        } catch {
            // Retries exhausted; the lease will lapse and the actor offers the task to a standby.
        }
    }, env.heartbeatMs)

    try {
        const previous = [...info.ledger]
        for (let stepId = info.nextStep; stepId <= env.totalSteps; stepId++) {
            const started = Date.now()
            const cutOff = info.cutOff && info.cutOff.stepId === stepId ? info.cutOff : null
            const draft = draftStreamer(taskId, attempt, stepId)
            let out
            try {
                out = await writeStep({ sheet, stepId, previous, directives: current.directives, worker: me, cutOff }, text => {
                    if (!hung) draft.push(text)
                })
            } finally {
                draft.stop()
            }
            const ms = Date.now() - started
            while (hung) await sleep(1000)
            const wait = env.stepPaceMs - ms
            if (wait > 0) await sleep(wait)
            const r = await callOrExit(() => task.checkpoint({ worker: me, attempt, step: { stepId, ...out, ms } }))
            if (!r.ok) {
                if (FENCED.has(r.reason)) fenced(taskId, r.reason)
                log(`checkpoint refused: ${r.reason}`)
                return
            }
            const stored = r.stored
            previous.push({ stepId, summary: stored.summary, worker: stored.worker })
            log(`step ${stepId}/${env.totalSteps}${r.duplicate ? " (already stored)" : ""}${cutOff ? " (finished the cut-off sentence)" : ""}`)
            if (env.transport === "relay" && client) {
                const meta: RelayMeta & { callbacks: string[]; worker: string } = { v: 1, type: "CKPT", runId: run.runId, taskId, attempt, stepId, from: me, correlationId: correlationId(run.runId, taskId, attempt), callbacks: stored.callbacks, worker: stored.worker }
                await send(client, `#${run.channel}`, `CKPT ${taskId} step ${stepId}/${env.totalSteps} by ${short} (attempt ${attempt}): ${stored.summary}`, meta, relayKey(run.runId, taskId, attempt, `ckpt:${stepId}`)).catch(e =>
                    log("CKPT send failed", e?.message),
                )
            }
        }
        const done = await callOrExit(() => task.complete({ worker: me, attempt }))
        if (!done.ok && FENCED.has(done.reason)) fenced(taskId, done.reason)
        log(`completed ${taskId}: ${done.ok ? "done" : done.reason}`)
    } finally {
        clearInterval(beat)
        current = null
    }
}

async function claimAndRun(taskId: string, attempt: number, via: string) {
    const key = `${taskId}:${attempt}`
    if (busy || handled.has(key)) return
    const token = Symbol(key)
    busy = token
    handled.add(key)
    try {
        const task = actors.Task.get(actorId(run.runId, taskId))
        const r = await callOrExit(() => task.claim({ worker: me, attempt }))
        if (!r.ok) {
            log(`claim ${taskId} attempt ${attempt} (${via}) refused: ${r.reason}`)
            return
        }
        if (attempt > 1 && !r.already) await announceTakeover(taskId, attempt, via, r.directives)
        await runTask(taskId, r)
    } finally {
        if (busy === token) busy = null
    }
}

// Agent to agent over Relay: tell the fleet who took the task, and tell the agent it replaced.
// The actor already enforces ownership; these messages explain it to the agents and humans.
async function announceTakeover(taskId: string, attempt: number, via: string, directives: Directive[]) {
    const s = await call(() => actors.Task.get(actorId(run.runId, taskId)).getState()).catch(() => null)
    const t = s?.takeover
    const from = t?.from ?? null
    const fromShort = (from ?? "?").replace(`${run.runId}-`, "")
    const why = (t?.why ?? s?.lostReason) === "stuck" ? "stuck" : "silent"
    const cut = s?.cutOff ? `, finishing its cut-off sentence "…${s.cutOff.text.slice(-40)}"` : ""
    const after = t?.afterMs != null ? ` ${(t.afterMs / 1000).toFixed(1)}s after it was lost` : ""
    const carry = directives.length ? `, keeping ${directives.length} human instruction${directives.length > 1 ? "s" : ""}` : ""
    log(`took ${taskId} attempt ${attempt} from ${fromShort} (${why}) via ${via}`)
    if (env.transport !== "relay" || !client) return
    const cid = correlationId(run.runId, taskId, attempt)
    await send(client, `#${run.channel}`, `TOOK ${taskId} attempt ${attempt} from ${fromShort} (${why}) at step ${t?.atStep ?? "?"}${after}${cut}${carry}`, { v: 1, type: "TOOK", runId: run.runId, taskId, attempt, from: me, correlationId: cid }, relayKey(run.runId, taskId, attempt, "took")).catch(e => log("TOOK send failed", e?.message))
    if (from) {
        await send(client, `@${from}`, `TOMBSTONE ${taskId}: attempt ${attempt} belongs to ${short} now. Stop writing; the actor will reject attempt ${attempt - 1}.`, { v: 1, type: "TOMBSTONE", runId: run.runId, taskId, attempt, from: me, correlationId: cid }, relayKey(run.runId, taskId, attempt, "tombstone")).catch(e => log("TOMBSTONE send failed", e?.message))
    }
    // The instruction outlives the agent it was given to: tell its author who carries it now.
    for (const author of new Set(directives.map(d => d.from))) {
        const mine = directives.filter(d => d.from === author).map(d => `"${d.text}"`).join(", ")
        await send(client, `@${author}`, `ACK ${taskId}: ${fromShort} is gone; I (${short}) took over at step ${t?.atStep ?? "?"} and I am keeping your instruction ${mine}.`, { v: 1, type: "ACK", runId: run.runId, taskId, attempt, from: me, correlationId: cid }, relayKey(run.runId, taskId, attempt, `ack-carry:${author}`)).catch(e => log("ACK send failed", e?.message))
    }
}

// A steer arrives as a Relay DM (from a human, or from the reviewer agent). Handshake: eyes on
// receipt, record it in the actor (fenced by attempt), then an ACK reply and a check mark.
async function onSteer(meta: RelayMeta & Record<string, unknown>, text: string, messageId: string) {
    const cur = current
    if (!cur || meta.taskId !== cur.taskId || !meta.directiveId) {
        log(`steer ${meta.directiveId ?? "?"} ignored: not working on ${meta.taskId}`)
        return
    }
    client?.react({ messageId, emoji: "eyes" }).catch(() => {})
    const directive = { id: meta.directiveId, text: text.replace(/^STEER \S+: /, ""), from: meta.from, at: Date.now() }
    const task = actors.Task.get(actorId(run.runId, cur.taskId))
    const r = await call(() => task.addDirective({ worker: me, attempt: cur.attempt, directive }))
    if (r.ok && current === cur && !cur.directives.some(d => d.id === directive.id)) cur.directives = [...cur.directives, directive]
    log(`steer ${directive.id} from ${String(meta.from).replace(`${run.runId}-`, "")}: ${r.ok ? "recorded" : r.reason}`)
    if (!r.ok || !client) return
    // A reviewer's note names the #fleet message to answer in, so the exchange stays public.
    const target = typeof meta.replyTo === "string" ? meta.replyTo : messageId
    const step = (await call(() => task.getState()).catch(() => null))?.ledgerLength ?? 0
    await client
        .reply({ messageId: target, text: `ACK ${cur.taskId} from ${short}: instruction recorded in the actor, applying it from step ${step + 1}.`, metadata: { v: 1, type: "ACK", runId: run.runId, taskId: cur.taskId, attempt: cur.attempt, from: me, correlationId: correlationId(run.runId, cur.taskId, cur.attempt) } })
        .catch(e => log("ACK reply failed", e?.message))
    client.react({ messageId, emoji: "white_check_mark" }).catch(() => {})
}

// One WebSocket per Task actor, tagged "standby". The actor pushes an `offer` when its lease
// expires, and its initial `state` shows a task that was lost before we connected.
function watchOffers(taskId: string, delay = 1000) {
    const retry = () => setTimeout(() => watchOffers(taskId, Math.min(delay * 2, 5000)), delay)
    actors.Task.prepareWebsocket({ actorId: actorId(run.runId, taskId), metadata: { viewer: me, role: "standby", name: me } })
        .then(({ websocketUrl }) => {
            const ws = new WebSocket(websocketUrl)
            ws.onopen = () => {
                delay = 1000
            }
            ws.onmessage = ev => {
                const m = JSON.parse(String(ev.data))
                if (m.type === "offer" && m.lostWorker !== me) {
                    log(`offer from actor ${taskId}: attempt ${m.attempt}, lost by ${String(m.lostWorker).replace(`${run.runId}-`, "")}`)
                    claimAndRun(m.taskId, m.attempt, "actor offer").catch(e => log("takeover failed", e?.message))
                } else if (m.type === "state" && m.state?.status === "lost" && m.state.lostWorker !== me) {
                    claimAndRun(taskId, m.state.attempt, "actor state").catch(e => log("takeover failed", e?.message))
                }
            }
            ws.onclose = retry
        })
        .catch(retry)
}

async function main() {
    log(`starting as ${role} (transport ${env.transport}, llm ${env.llmMode})`)
    if (env.transport === "relay") {
        try {
            const r = relay()
            client = await joinAs(r, me)
            await joinChannel(client, run.channel)
            r.addListener("dm.received", async (event: any) => {
                const message = event.message
                const meta = await readMeta(client!, message.id)
                if (!meta || meta.runId !== run.runId) return
                if (meta.type === "STEER") onSteer(meta, message.text, message.id).catch(e => log("steer failed", e?.message))
                else if (meta.type === "TOMBSTONE") log(`TOMBSTONE from ${String(meta.from).replace(`${run.runId}-`, "")}: ${message.text}`)
            })
        } catch (e: any) {
            client = null
            log(`Relay unavailable (${e?.message}); continuing without #fleet messages`)
        }
    }

    if (role === "worker") {
        await claimAndRun(firstTask!, 1, "start")
        process.exit(0)
    }

    for (const t of run.tasks) watchOffers(t.id)

    // Backstop for a missed offer: find lost tasks by polling, and expire a lease whose alarm
    // was dropped (a crash of the actor runtime loses pending alarms). With TRANSPORT=poll
    // this is the only path, on a faster period.
    const period = env.transport === "poll" ? env.pollMs : 3000
    for (;;) {
        await sleep(period)
        if (busy) continue
        for (const t of run.tasks) {
            try {
                const task = actors.Task.get(actorId(run.runId, t.id))
                const s = await call(() => task.getState())
                if (s.status === "running" && s.leaseDeadline !== null && s.now > s.leaseDeadline + 2000) {
                    const r = await call(() => task.expire({ attempt: s.attempt }))
                    if (r.ok) log(`expired ${t.id} attempt ${s.attempt} (alarm missed)`)
                } else if (s.status === "lost" && s.lostWorker !== me) {
                    await claimAndRun(t.id, s.attempt, "poll")
                }
            } catch {
                // Actor briefly unreachable; try again next round.
            }
        }
    }
}

main().catch(e => {
    log("fatal", e?.message ?? e)
    process.exit(1)
})

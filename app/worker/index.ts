// A worker agent process. Usage:
//   bun run worker/index.ts worker w1 t1    (owns task t1 from attempt 1)
//   bun run worker/index.ts standby s1      (waits for a HANDOFF DM, then continues that task)
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
type Current = { taskId: string; attempt: number; directives: Directive[] }

let current: Current | null = null
const handled = new Set<string>()
let client: RelayAgentClient | null = null

const FENCED = new Set(["stale-attempt", "wrong-owner"])

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

async function runTask(taskId: string, attempt: number, ledger: Ledger, directives: Directive[], nextStep: number) {
    const task = actors.Task.get(actorId(run.runId, taskId))
    const sheet = FACT_SHEETS.find(s => s.id === taskId)!
    current = { taskId, attempt, directives: [...directives] }
    log(`running ${taskId} attempt ${attempt} from step ${nextStep}`)

    // The heartbeat has its own timer so a slow LLM call does not expire the lease.
    const beat = setInterval(async () => {
        try {
            const r = await call(() => task.heartbeat({ worker: me, attempt }))
            if (!r.ok && FENCED.has(r.reason)) fenced(taskId, r.reason)
        } catch {
            // Retries exhausted; the lease will lapse and the bridge hands the task off.
        }
    }, env.heartbeatMs)

    try {
        const previous = [...ledger]
        for (let stepId = nextStep; stepId <= env.totalSteps; stepId++) {
            const started = Date.now()
            const out = await writeStep({ sheet, stepId, previous, directives: current.directives, worker: me })
            const wait = env.stepPaceMs - (Date.now() - started)
            if (wait > 0) await sleep(wait)
            const r = await callOrExit(() => task.checkpoint({ worker: me, attempt, step: { stepId, ...out } }))
            if (!r.ok) {
                if (FENCED.has(r.reason)) fenced(taskId, r.reason)
                log(`checkpoint refused: ${r.reason}`)
                return
            }
            const stored = r.stored
            previous.push({ stepId, summary: stored.summary, worker: stored.worker })
            log(`step ${stepId}/${env.totalSteps}${r.duplicate ? " (already stored)" : ""}`)
            if (env.transport === "relay" && client) {
                const meta: RelayMeta = { v: 1, type: "CKPT", runId: run.runId, taskId, attempt, stepId, from: me, correlationId: correlationId(run.runId, taskId, attempt) }
                await send(client, `#${run.channel}`, `CKPT ${taskId} step ${stepId}/${env.totalSteps} by ${short}: ${stored.summary}`, meta, relayKey(run.runId, taskId, attempt, `ckpt:${stepId}`)).catch(e =>
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

async function claimAndRun(taskId: string, attempt: number) {
    const key = `${taskId}:${attempt}`
    if (handled.has(key) || current) return
    const task = actors.Task.get(actorId(run.runId, taskId))
    const r = await callOrExit(() => task.claim({ worker: me, attempt }))
    handled.add(key)
    if (!r.ok) {
        log(`claim ${taskId} attempt ${attempt} refused: ${r.reason}`)
        return
    }
    await runTask(taskId, attempt, r.ledger, r.directives, r.nextStep)
}

async function onSteer(meta: RelayMeta & Record<string, unknown>, text: string) {
    const cur = current
    if (!cur || meta.taskId !== cur.taskId || !meta.directiveId) {
        log(`steer ${meta.directiveId ?? "?"} ignored: not working on ${meta.taskId}`)
        return
    }
    const directive = { id: meta.directiveId, text: text.replace(/^STEER \S+: /, ""), from: meta.from, at: Date.now() }
    const task = actors.Task.get(actorId(run.runId, cur.taskId))
    const r = await call(() => task.addDirective({ worker: me, attempt: cur.attempt, directive }))
    if (r.ok && !r.duplicate && current === cur) cur.directives = [...cur.directives, directive]
    log(`steer ${directive.id}: ${r.ok ? "recorded" : r.reason}`)
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
            const meta = (await readMeta(client!, message.id)) ?? (message.from?.name === agentName(run.runId, "bridge") ? parseText(message.text) : null)
            if (!meta) {
                log(`DM ${message.id} without readable metadata, ignored`)
                return
            }
            if (meta.runId !== run.runId) return
            if (meta.type === "HANDOFF" && role === "standby") {
                log(`HANDOFF received for ${meta.taskId} attempt ${meta.attempt}`)
                claimAndRun(meta.taskId, meta.attempt).catch(e => log("handoff run failed", e?.message))
            } else if (meta.type === "STEER") {
                onSteer(meta, message.text).catch(e => log("steer failed", e?.message))
            }
            })
        } catch (e: any) {
            client = null
            log(`Relay unavailable (${e?.message}); continuing without checkpoints on #fleet`)
        }
    }

    if (role === "worker") {
        await claimAndRun(firstTask!, 1)
        // Stay available as a standby after finishing the first task.
    }

    // Polling for tasks offered to this agent: the only path with TRANSPORT=poll (Afterlife-solo),
    // and a slower backstop for a missed HANDOFF DM with Relay.
    const period = env.transport === "poll" ? env.pollMs : 3000
    for (;;) {
        if (!current) {
            for (const t of run.tasks) {
                try {
                    const s = await call(() => actors.Task.get(actorId(run.runId, t.id)).getState())
                    if (s.status === "handing-off" && s.assignee === me) {
                        if (env.transport === "relay") log(`found ${t.id} offered to me by polling (no DM yet)`)
                        await claimAndRun(t.id, s.attempt)
                    }
                } catch {
                    // Actor briefly unreachable; try again next round.
                }
            }
        }
        await sleep(period)
    }
}

// Fallback when metadata cannot be read back: the bridge's HANDOFF text carries task and attempt.
function parseText(text: string): (RelayMeta & Record<string, unknown>) | null {
    const m = /^HANDOFF (\S+) attempt (\d+):/.exec(text ?? "")
    if (!m) return null
    const attempt = Number(m[2])
    return { v: 1, type: "HANDOFF", runId: run.runId, taskId: m[1]!, attempt, from: "text", correlationId: correlationId(run.runId, m[1]!, attempt) }
}

main().catch(e => {
    log("fatal", e?.message ?? e)
    process.exit(1)
})

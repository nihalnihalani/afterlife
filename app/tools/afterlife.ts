// Afterlife CLI for an agent that cannot run our worker loop, such as a Claude Code session
// acting as a standby. Every write carries the attempt number, so the Task actor fences it
// exactly like a worker process. Prints JSON; a fenced or refused write prints STOP and exits 3.
//
//   bun run app/tools/afterlife.ts offers
//   bun run app/tools/afterlife.ts wait [--timeout 600]           (blocks until an actor offers a lost task)
//   bun run app/tools/afterlife.ts claim <task> <attempt> --as cc [--lease-ms 120000]
//   bun run app/tools/afterlife.ts checkpoint <task> <attempt> --as cc --step N --summary "..." --text "..." [--callbacks "a;b"]
//   bun run app/tools/afterlife.ts heartbeat <task> <attempt> --as cc
//   bun run app/tools/afterlife.ts complete <task> <attempt> --as cc
//   bun run app/tools/afterlife.ts state <task>
import { actors } from "../generated/index.js"
import { actorId, agentName, correlationId, env, loadRun, relayKey } from "../shared/config"
import { joinAs, joinChannel, relay, send } from "../shared/relay"
import { FACT_SHEETS, SECTIONS } from "../worker/data"

const run = loadRun()
const [cmd, ...rest] = process.argv.slice(2)
const positional = rest.filter((a, i) => !a.startsWith("--") && !rest[i - 1]?.startsWith("--"))
const flag = (name: string) => {
    const i = rest.indexOf(`--${name}`)
    return i === -1 ? undefined : rest[i + 1]
}
const print = (v: unknown) => console.log(JSON.stringify(v, null, 2))
function stop(reason: string): never {
    console.log(`STOP: ${reason}. Another agent owns this task, or it cannot be written now. Do not write to it again.`)
    process.exit(3)
}
function usage(): never {
    console.error("usage: afterlife.ts offers | wait | claim|checkpoint|heartbeat|complete <task> <attempt> --as <name> | state <task>")
    process.exit(2)
}

const taskArg = () => positional[0] ?? usage()
const attemptArg = () => Number(positional[1] ?? usage())
const who = () => agentName(run.runId, flag("as") ?? usage())
const task = (id: string) => actors.Task.get(actorId(run.runId, id))
const short = (name: string | null) => (name ?? "?").replace(`${run.runId}-`, "")

async function offers() {
    const out = []
    for (const t of run.tasks) {
        const s = await task(t.id).getState()
        if (s.status === "lost") out.push({ task: t.id, attempt: s.attempt, lostWorker: short(s.lostWorker), nextStep: s.ledgerLength + 1, cutOff: !!s.cutOff })
    }
    return out
}

// Same path as the standby processes: a WebSocket per Task actor, tagged "standby".
async function wait() {
    const already = await offers()
    if (already.length) return print(already[0])
    const timeout = Number(flag("timeout") ?? 600) * 1000
    const sockets: WebSocket[] = []
    await new Promise<void>(done => {
        const timer = setTimeout(() => {
            console.log("no offer before the timeout")
            done()
        }, timeout)
        for (const t of run.tasks) {
            actors.Task.prepareWebsocket({ actorId: actorId(run.runId, t.id), metadata: { viewer: "cli", role: "standby", name: "cli" } }).then(({ websocketUrl }) => {
                const ws = new WebSocket(websocketUrl)
                sockets.push(ws)
                ws.onmessage = ev => {
                    const m = JSON.parse(String(ev.data))
                    if (m.type !== "offer") return
                    clearTimeout(timer)
                    print({ task: m.taskId, attempt: m.attempt, lostWorker: short(m.lostWorker), nextStep: m.nextStep })
                    done()
                }
            })
        }
    })
    for (const ws of sockets) ws.close()
}

async function claim() {
    const id = taskArg()
    const attempt = attemptArg()
    const worker = who()
    const r = await task(id).claim({ worker, attempt, leaseMs: Number(flag("lease-ms") ?? 120000) })
    if (!r.ok) stop(r.reason)
    if (attempt > 1 && !r.already) await announce(id, attempt, worker)
    const sheet = FACT_SHEETS.find(s => s.id === id)!
    print({
        task: id,
        attempt: r.attempt,
        you: short(worker),
        leaseSeconds: r.leaseMs / 1000,
        site: sheet.title,
        facts: sheet.facts,
        sections: SECTIONS,
        savedSteps: r.ledger.map(c => ({ step: c.stepId, section: SECTIONS[c.stepId - 1], by: short(c.worker), summary: c.summary })),
        cutOff: r.cutOff ? { step: r.cutOff.stepId, by: short(r.cutOff.worker), text: r.cutOff.text } : null,
        directives: r.directives.map(d => d.text),
        nextStep: r.nextStep,
        instructions:
            `Write steps ${r.nextStep}..${env.totalSteps}, 70-110 words each, using only these facts and continuing the saved steps.` +
            (r.cutOff ? ` Step ${r.cutOff.stepId} was cut off mid-sentence: pass --text as the cut-off text followed by your continuation.` : "") +
            ` Save each with checkpoint before writing the next. Each save renews your ${r.leaseMs / 1000}s lease.`,
    })
}

// Agent to agent over Relay, like the standby processes: tell #fleet, tell the agent replaced.
async function announce(id: string, attempt: number, worker: string) {
    if (env.transport !== "relay") return
    try {
        const s = await task(id).getState()
        const client = await joinAs(relay(), worker)
        await joinChannel(client, run.channel)
        const cid = correlationId(run.runId, id, attempt)
        const cut = s.cutOff ? `, finishing its cut-off sentence` : ""
        await send(client, `#${run.channel}`, `TOOK ${id} attempt ${attempt} from ${short(s.takeover?.from ?? null)} at step ${s.takeover?.atStep ?? "?"}${cut} (Claude Code session)`, { v: 1, type: "TOOK", runId: run.runId, taskId: id, attempt, from: worker, correlationId: cid }, relayKey(run.runId, id, attempt, "took"))
        if (s.takeover?.from) await send(client, `@${s.takeover.from}`, `TOMBSTONE ${id}: attempt ${attempt} belongs to ${short(worker)} now. Stop writing.`, { v: 1, type: "TOMBSTONE", runId: run.runId, taskId: id, attempt, from: worker, correlationId: cid }, relayKey(run.runId, id, attempt, "tombstone"))
    } catch (e: any) {
        console.error(`Relay announce failed (the claim still holds): ${e?.message}`)
    }
}

async function checkpoint() {
    const id = taskArg()
    const attempt = attemptArg()
    const stepId = Number(flag("step") ?? usage())
    const text = flag("text") ?? usage()
    const summary = flag("summary") ?? text.split(/(?<=[.!?])\s/)[0] ?? ""
    const callbacks = (flag("callbacks") ?? "").split(";").map(s => s.trim()).filter(Boolean)
    const r = await task(id).checkpoint({ worker: who(), attempt, step: { stepId, summary, text, callbacks } })
    if (!r.ok) stop(r.reason)
    print({ saved: stepId, duplicate: r.duplicate, nextStep: r.nextStep, finishedCutOff: !!r.stored?.continued })
}

async function heartbeat() {
    const r = await task(taskArg()).heartbeat({ worker: who(), attempt: attemptArg() })
    if (!r.ok) stop(r.reason)
    print({ leaseUntil: new Date(r.leaseDeadline!).toISOString() })
}

async function complete() {
    const r = await task(taskArg()).complete({ worker: who(), attempt: attemptArg() })
    if (!r.ok) stop(r.reason)
    print({ done: true })
}

const commands: Record<string, () => Promise<unknown>> = {
    offers: async () => print(await offers()),
    wait,
    claim,
    checkpoint,
    heartbeat,
    complete,
    state: async () => print(await task(taskArg()).getState()),
}
await (commands[cmd ?? ""] ?? usage)()
process.exit(0)

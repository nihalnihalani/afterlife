// The bridge: an observer, not a supervisor. Task actors offer lost tasks to standby agents
// themselves, so killing the bridge never stops a handoff. It announces LOST and DONE on
// #fleet, mirrors #fleet into a live feed for the board, issues WebSocket grants, forwards
// steering, and (with CLAUDE_STANDBY set) DMs a HANDOFF prompt to a Claude Code session.
import { resolve } from "node:path"
import type { RelayAgentClient } from "@agent-relay/sdk"
import { actors } from "../generated/index.js"
import { actorId, agentName, correlationId, env, loadRun, relayKey, sleep, type RelayMeta } from "../shared/config"
import { joinAs, joinChannel, relay, send } from "../shared/relay"

const run = loadRun()
const me = agentName(run.runId, "bridge")
const human = agentName(run.runId, "human")
// Relay name of a Claude Code session acting as a standby (see app/tools/README.md).
const claudeStandby = process.env.CLAUDE_STANDBY ?? ""
const bootedAt = Date.now()
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), "bridge", ...a)

type FeedItem = { at: number; type: string; from: string; to: string; text: string }
const feed: FeedItem[] = []
const listeners = new Set<(item: FeedItem) => void>()
function pushFeed(item: FeedItem) {
    feed.push(item)
    if (feed.length > 60) feed.shift()
    for (const l of listeners) l(item)
}

const taskOf = (taskId: string) => actors.Task.get(actorId(run.runId, taskId))
type State = Awaited<ReturnType<ReturnType<typeof taskOf>["getState"]>>
const announced = new Set<string>()

let client: RelayAgentClient | null = null
let humanClient: RelayAgentClient | null = null

async function announce(to: string, text: string, meta: RelayMeta, key: string) {
    if (!client) return
    const sent = await send(client, to, text, meta, key)
    pushFeed({ at: Date.now(), type: meta.type, from: me, to, text: sent === null ? `${text} (already sent)` : text })
}

// Announces each event once per bridge process; Relay idempotency keys make a restarted
// bridge's repeat a no-op on the server.
async function once(key: string, fn: () => Promise<void>) {
    if (announced.has(key)) return
    announced.add(key)
    await fn().catch(e => {
        announced.delete(key)
        throw e
    })
}

async function observe(s: State) {
    const cid = correlationId(run.runId, s.taskId, s.attempt)
    // Safety net: an expired lease that the alarm did not handle (G1: a runtime crash drops alarms).
    // Standbys run the same check, so this is not needed for progress.
    if (s.status === "running" && s.leaseDeadline !== null && s.now > s.leaseDeadline + 2000) {
        const r = await taskOf(s.taskId).expire({ attempt: s.attempt })
        if (r.ok) log(`expired ${s.taskId} attempt ${s.attempt} (alarm missed)`)
        return
    }
    if (s.status === "lost") {
        const lost = (s.lostWorker ?? "?").replace(`${run.runId}-`, "")
        await once(`lost:${s.taskId}:${s.attempt}`, () =>
            announce(`#${run.channel}`, `LOST ${s.taskId}: ${lost} went silent; the actor offered attempt ${s.attempt} to standbys`, { v: 1, type: "LOST", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: cid }, relayKey(run.runId, s.taskId, s.attempt, "lost")),
        )
        if (claudeStandby && env.transport === "relay") {
            await once(`handoff:${s.taskId}:${s.attempt}`, () => announce(`@${claudeStandby}`, handoffPrompt(s), { v: 1, type: "HANDOFF", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: cid }, relayKey(run.runId, s.taskId, s.attempt, "handoff")))
        }
    } else if (s.status === "done") {
        const by = (s.assignee ?? "?").replace(`${run.runId}-`, "")
        await once(`done:${s.taskId}`, () =>
            announce(`#${run.channel}`, `DONE ${s.taskId} by ${by} (${s.ledgerLength}/${s.totalSteps})`, { v: 1, type: "DONE", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: cid }, relayKey(run.runId, s.taskId, s.attempt, "done")),
        )
    }
}

// A self-contained instruction for a Claude Code session that receives this DM as a turn.
function handoffPrompt(s: State) {
    const tool = `bun run ${resolve(import.meta.dir, "../tools/afterlife.ts")}`
    const lost = (s.lostWorker ?? "?").replace(`${run.runId}-`, "")
    return [
        `HANDOFF ${s.taskId} attempt ${s.attempt}: agent ${lost} died at step ${s.ledgerLength + 1} of ${s.totalSteps}${s.cutOff ? " in the middle of a sentence" : ""}. Please take over this Afterlife task.`,
        `1. Claim it: ${tool} claim ${s.taskId} ${s.attempt} --as cc  (prints the fact sheet, the saved steps and any cut-off sentence; if it prints STOP, another agent won).`,
        `2. For each remaining step, write a 70-110 word section that continues the earlier ones (finish the cut-off sentence first, without repeating it), then save it: ${tool} checkpoint ${s.taskId} ${s.attempt} --as cc --step N --summary "<one sentence>" --text "<section>"`,
        `3. Finish: ${tool} complete ${s.taskId} ${s.attempt} --as cc. If any command prints STOP, stop at once.`,
    ].join("\n")
}

async function tick() {
    for (const t of run.tasks) {
        try {
            await observe(await taskOf(t.id).getState())
        } catch (e: any) {
            log(`observing ${t.id} failed: ${e?.message}`)
        }
    }
}

async function steer(taskId: string, raw: string) {
    const text = raw.replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, 200)
    const s = await taskOf(taskId).getState()
    if (!humanClient || !text || s.status !== "running" || !s.assignee) return { ok: false, reason: "task-not-running" }
    const directiveId = `d${Date.now().toString(36)}`
    const meta: RelayMeta = { v: 1, type: "STEER", runId: run.runId, taskId, attempt: s.attempt, directiveId, from: human, correlationId: correlationId(run.runId, taskId, s.attempt) }
    await send(humanClient, `@${s.assignee}`, `STEER ${taskId}: ${text}`, meta, `${run.runId}-${taskId}-steer-${directiveId}`)
    pushFeed({ at: Date.now(), type: "STEER", from: human, to: s.assignee, text: `STEER ${taskId} sent: ${text}` })
    return { ok: true, directiveId }
}

async function main() {
    if (env.transport === "relay") {
        const r = relay()
        client = await joinAs(r, me)
        humanClient = await joinAs(r, human)
        await joinChannel(client, run.channel)
        r.addListener("message.created", (event: any) => {
            const m = event.message
            if (m?.channel?.name !== run.channel || m?.from?.name === me) return
            pushFeed({ at: Date.now(), type: String(m.text).split(" ")[0] ?? "MSG", from: m.from?.name ?? "?", to: `#${run.channel}`, text: m.text })
        })
    }
    for (const t of run.tasks) {
        const r = await taskOf(t.id).rearm()
        if (r.ok) log(`re-armed ${t.id} lease`)
    }
    log(`up: run ${run.runId}, transport ${env.transport}, board http://127.0.0.1:${env.bridgePort}${claudeStandby ? `, Claude standby @${claudeStandby}` : ""}`)
    ;(async () => {
        for (;;) {
            await tick().catch(e => log("poll error", e?.message))
            await sleep(env.pollMs)
        }
    })()
}

const boardFile = resolve(import.meta.dir, "../board/index.html")

Bun.serve({
    port: env.bridgePort,
    hostname: "127.0.0.1",
    idleTimeout: 0,
    async fetch(req) {
        const url = new URL(req.url)
        // Localhost only: reject other Host headers (DNS rebinding) before serving anything.
        const host = req.headers.get("host") ?? ""
        if (host !== `127.0.0.1:${env.bridgePort}` && host !== `localhost:${env.bridgePort}`) return new Response("forbidden", { status: 403 })
        if (url.pathname === "/") return new Response(Bun.file(boardFile), { headers: { "Content-Type": "text/html; charset=utf-8" } })
        if (url.pathname === "/api/run") {
            return Response.json({ runId: run.runId, channel: run.channel, tasks: run.tasks, transport: env.transport, llmMode: env.llmMode, model: env.openaiModel, stepPaceMs: env.stepPaceMs, bridgeBootedAt: bootedAt, steer: process.env.STEER_ENABLED === "1", claudeStandby: claudeStandby || null })
        }
        if (url.pathname === "/api/grants") {
            const grants = await Promise.all(run.tasks.map(async t => ({ taskId: t.id, ...(await actors.Task.prepareWebsocket({ actorId: actorId(run.runId, t.id), metadata: { viewer: "board", role: "board" } })) })))
            return Response.json(grants, { headers: { "Cache-Control": "no-store" } })
        }
        if (url.pathname === "/api/feed") {
            let cleanup = () => {}
            const stream = new ReadableStream({
                start(controller) {
                    const write = (item: FeedItem) => controller.enqueue(`data: ${JSON.stringify(item)}\n\n`)
                    for (const item of feed) write(item)
                    listeners.add(write)
                    const ping = setInterval(() => controller.enqueue(": ping\n\n"), 15000)
                    cleanup = () => {
                        listeners.delete(write)
                        clearInterval(ping)
                    }
                },
                cancel() {
                    cleanup()
                },
            })
            return new Response(stream, { headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-store" } })
        }
        if (url.pathname === "/api/steer" && req.method === "POST") {
            if (process.env.STEER_ENABLED !== "1") return Response.json({ ok: false, reason: "steer-disabled" }, { status: 403 })
            if (!req.headers.get("content-type")?.includes("application/json")) return Response.json({ ok: false, reason: "json-only" }, { status: 415 })
            const body = (await req.json().catch(() => ({}))) as { taskId?: string; text?: string }
            if (!body.taskId || !run.tasks.some(t => t.id === body.taskId)) return Response.json({ ok: false, reason: "unknown-task" }, { status: 400 })
            return Response.json(await steer(body.taskId, String(body.text ?? "")))
        }
        return new Response("not found", { status: 404 })
    },
})

main().catch(e => {
    log("fatal", e?.message ?? e)
    process.exit(1)
})

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
        const why = s.lostReason === "stuck" ? "is stuck (heartbeating, no progress)" : "went silent"
        await once(`lost:${s.taskId}:${s.attempt}`, () =>
            announce(`#${run.channel}`, `LOST ${s.taskId}: ${lost} ${why}; the actor offered attempt ${s.attempt} to standbys`, { v: 1, type: "LOST", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: cid }, relayKey(run.runId, s.taskId, s.attempt, "lost")),
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

// The instruction is recorded in the Task actor first, then delivered over Relay. If the agent
// dies before reading it, whoever takes over still gets it from the actor.
async function steer(taskId: string, raw: string) {
    const text = raw.replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, 200)
    if (!humanClient || !text) return { ok: false, reason: "steer-unavailable" }
    const directiveId = `d${Date.now().toString(36)}`
    const r = await taskOf(taskId).steer({ id: directiveId, text, from: human })
    if (!r.ok) return r
    const meta: RelayMeta = { v: 1, type: "STEER", runId: run.runId, taskId, attempt: r.attempt, directiveId, from: human, correlationId: correlationId(run.runId, taskId, r.attempt) }
    await send(humanClient, `@${r.assignee}`, `STEER ${taskId}: ${text}`, meta, `${run.runId}-${taskId}-steer-${directiveId}`)
    pushFeed({ at: Date.now(), type: "STEER", from: human, to: r.assignee, text: `STEER ${taskId}: "${text}" (recorded in the actor, then sent)` })
    return { ok: true, directiveId }
}

const EMOJI: Record<string, string> = { eyes: "👀", white_check_mark: "✅" }
const seen = new Set<string>()

// A restarted bridge has an empty feed, but the agents kept talking while it was down. Relay
// keeps the channel history, so read #fleet and its threads back into the feed.
async function backfill(c: RelayAgentClient) {
    try {
        const messages = await c.messages.list(run.channel, { limit: 60 })
        const items: FeedItem[] = []
        for (const m of messages) {
            const at = m.createdAt ? Date.parse(String(m.createdAt)) : Date.now()
            const from = m.from?.name ?? "?"
            if (!seen.has(m.id) && from !== me && from.startsWith(`${run.runId}-`)) {
                seen.add(m.id)
                items.push({ at, type: String(m.text).split(" ")[0] ?? "MSG", from, to: `#${run.channel}`, text: m.text })
            }
            if (!m.replyCount) continue
            const thread = await c.threads.get(m.id).catch(() => null)
            for (const reply of thread?.replies ?? []) {
                if (seen.has(reply.id)) continue
                seen.add(reply.id)
                items.push({ at: reply.createdAt ? Date.parse(String(reply.createdAt)) : at, type: String(reply.text).split(" ")[0] ?? "MSG", from: reply.from?.name ?? "?", to: `#${run.channel} thread`, text: reply.text })
            }
        }
        items.sort((a, b) => a.at - b.at)
        for (const item of items) pushFeed(item)
        if (items.length) pushFeed({ at: Date.now(), type: "BACKFILL", from: me, to: "-", text: `read ${items.length} messages back from Relay, including what the agents said to each other while the bridge was down` })
    } catch (e: any) {
        log(`backfill failed: ${e?.message}`)
    }
}

async function main() {
    if (env.transport === "relay") {
        const r = relay()
        client = await joinAs(r, me)
        humanClient = await joinAs(r, human)
        await joinChannel(client, run.channel)
        r.addListener("message.created", (event: any) => {
            const m = event.message
            if (m?.channel?.name !== run.channel || m?.from?.name === me || seen.has(m.id)) return
            seen.add(m.id)
            pushFeed({ at: Date.now(), type: String(m.text).split(" ")[0] ?? "MSG", from: m.from?.name ?? "?", to: `#${run.channel}`, text: m.text })
        })
        // Agent-to-agent traffic the board should show: thread replies on #fleet (reviews, ACKs),
        // DMs and replies addressed to the human, and reactions.
        const mirror = (to: string) => (event: any) => {
            const m = event.message
            if (!m?.id || seen.has(m.id) || m?.from?.name === me || m?.from?.name === human) return
            if (to !== "human" && m?.channel?.name && m.channel.name !== run.channel) return
            if (!String(m.from?.name ?? "").startsWith(`${run.runId}-`)) return
            seen.add(m.id)
            pushFeed({ at: Date.now(), type: String(m.text).split(" ")[0] ?? "MSG", from: m.from?.name ?? "?", to: to === "human" ? human : `#${run.channel} thread`, text: m.text })
        }
        r.addListener("thread.reply", mirror("thread"))
        r.addListener("dm.received", mirror("human"))
        r.addListener("message.reacted", (event: any) => {
            const who = String(event.agentName ?? "?")
            const emoji = String(event.emoji ?? "?")
            if (event.action === "removed" || !who.startsWith(`${run.runId}-`) || who === me) return
            pushFeed({ at: Date.now(), type: "REACT", from: who, to: "-", text: `${EMOJI[emoji] ?? emoji} ${emoji}` })
        })
        await backfill(client)
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
            return Response.json({ runId: run.runId, channel: run.channel, tasks: run.tasks, transport: env.transport, llmMode: env.llmMode, model: env.openaiModel, stepPaceMs: env.stepPaceMs, bridgeBootedAt: bootedAt, steer: process.env.STEER_ENABLED !== "0", claudeStandby: claudeStandby || null, controlPort: env.controlPort })
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
            if (process.env.STEER_ENABLED === "0") return Response.json({ ok: false, reason: "steer-disabled" }, { status: 403 })
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

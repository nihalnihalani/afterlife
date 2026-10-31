// The bridge: stateless supervisor. It polls Task actors, hands lost tasks to an idle standby
// over Relay, mirrors #fleet into a live feed for the board, and issues WebSocket grants.
// Killing it is safe: leases keep expiring inside the actors without it.
import { resolve } from "node:path"
import type { RelayAgentClient } from "@agent-relay/sdk"
import { actors } from "../generated/index.js"
import { actorId, agentName, correlationId, env, loadRun, relayKey, sleep, type RelayMeta } from "../shared/config"
import { joinAs, joinChannel, relay, send } from "../shared/relay"

const run = loadRun()
const me = agentName(run.runId, "bridge")
const human = agentName(run.runId, "human")
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
const states = new Map<string, State>()
const announcedDone = new Set<string>()
const announcedLost = new Set<string>()
const waitingNoted = new Set<string>()

let client: RelayAgentClient | null = null
let humanClient: RelayAgentClient | null = null

async function announce(to: string, text: string, meta: RelayMeta, key: string) {
    if (!client) return
    const sent = await send(client, to, text, meta, key)
    pushFeed({ at: Date.now(), type: meta.type, from: me, to, text: sent === null ? `${text} (already sent)` : text })
}

function idleStandby(lostWorker: string | null): string | null {
    const busy = new Set([...states.values()].filter(s => s.status === "running" || s.status === "handing-off").map(s => s.assignee))
    const candidates = run.standbys.map(s => agentName(run.runId, s)).filter(s => !busy.has(s) && s !== lostWorker)
    return candidates[0] ?? null
}

async function handOff(s: State) {
    const key = `${s.taskId}:${s.attempt}`
    if (!announcedLost.has(key)) {
        const shortLost = (s.lostWorker ?? "?").replace(`${run.runId}-`, "")
        const cid = correlationId(run.runId, s.taskId, s.attempt)
        await announce(`#${run.channel}`, `LOST ${s.taskId}: ${shortLost} went silent; now attempt ${s.attempt}`, { v: 1, type: "LOST", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: cid }, relayKey(run.runId, s.taskId, s.attempt, "lost"))
        announcedLost.add(key)
    }
    const standby = idleStandby(s.lostWorker)
    if (!standby) {
        if (!waitingNoted.has(key)) {
            waitingNoted.add(key)
            pushFeed({ at: Date.now(), type: "WAIT", from: me, to: "-", text: `${s.taskId} attempt ${s.attempt}: waiting for a free standby` })
        }
        return
    }
    const r = await taskOf(s.taskId).beginHandoff({ standby, attempt: s.attempt })
    if (!r.ok) return
    // Mark the standby busy now so a second lost task in the same poll picks a different one.
    states.set(s.taskId, { ...s, status: "handing-off", assignee: standby })
    await sendHandoff(s.taskId, s.attempt, standby, r.packet.ledger)
}

async function sendHandoff(taskId: string, attempt: number, standby: string, ledger: { stepId: number; summary: string }[]) {
    const done = ledger.map(c => `${c.stepId}) ${c.summary.slice(0, 80)}`).join(" ")
    const text = `HANDOFF ${taskId} attempt ${attempt}: continue from step ${ledger.length + 1}. Done so far: ${done || "nothing"}`
    const meta: RelayMeta = { v: 1, type: "HANDOFF", runId: run.runId, taskId, attempt, from: me, correlationId: correlationId(run.runId, taskId, attempt) }
    if (env.transport === "relay") await announce(`@${standby}`, text, meta, relayKey(run.runId, taskId, attempt, "handoff"))
    else pushFeed({ at: Date.now(), type: "HANDOFF", from: me, to: standby, text: `${text} (poll transport)` })
    await taskOf(taskId).markHandoffSent({ attempt })
}

async function tick() {
    for (const t of run.tasks) {
        try {
            states.set(t.id, await taskOf(t.id).getState())
        } catch (e: any) {
            log(`reading ${t.id} failed: ${e?.message}`)
        }
    }
    for (const s of [...states.values()]) {
        try {
            await supervise(s)
        } catch (e: any) {
            log(`supervising ${s.taskId} failed: ${e?.message}`)
        }
    }
}

async function supervise(s: State) {
    const leased = s.status === "running" || s.status === "handing-off"
    // Safety net: an expired lease that the alarm did not handle (G1: a runtime crash drops alarms).
    if (leased && s.leaseDeadline !== null && s.now > s.leaseDeadline + 2000) {
        const r = await taskOf(s.taskId).expire({ attempt: s.attempt })
        if (r.ok) log(`expired ${s.taskId} attempt ${s.attempt} (alarm missed)`)
        return
    }
    if (s.status === "lost") await handOff(s)
    else if (s.status === "handing-off" && s.handoff && !s.handoff.dmSent) {
        const packet = await taskOf(s.taskId).beginHandoff({ standby: s.handoff.to, attempt: s.handoff.attempt })
        if (packet.ok) await sendHandoff(s.taskId, s.handoff.attempt, s.handoff.to, packet.packet.ledger)
    } else if (s.status === "done" && !announcedDone.has(s.taskId)) {
        const by = (s.assignee ?? "?").replace(`${run.runId}-`, "")
        await announce(`#${run.channel}`, `DONE ${s.taskId} by ${by} (${s.ledgerLength}/${s.totalSteps})`, { v: 1, type: "DONE", runId: run.runId, taskId: s.taskId, attempt: s.attempt, from: me, correlationId: correlationId(run.runId, s.taskId, s.attempt) }, relayKey(run.runId, s.taskId, s.attempt, "done"))
        announcedDone.add(s.taskId)
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
    log(`up: run ${run.runId}, transport ${env.transport}, board http://127.0.0.1:${env.bridgePort}`)
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
            return Response.json({ runId: run.runId, channel: run.channel, tasks: run.tasks, transport: env.transport, llmMode: env.llmMode, model: env.openaiModel, stepPaceMs: env.stepPaceMs, bridgeBootedAt: bootedAt, steer: process.env.STEER_ENABLED === "1" })
        }
        if (url.pathname === "/api/grants") {
            const grants = await Promise.all(run.tasks.map(async t => ({ taskId: t.id, ...(await actors.Task.prepareWebsocket({ actorId: actorId(run.runId, t.id), metadata: { viewer: "board" } })) })))
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

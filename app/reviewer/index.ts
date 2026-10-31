// The reviewer: a peer agent that only talks over Agent Relay. It reads checkpoints on #fleet,
// and when the first checkpoint of a new attempt lands (an agent took over a dead one's task),
// it reviews continuity in that checkpoint's thread and DMs the new author a STEER note. The
// author records the note in the Task actor and answers with an ACK in the same thread.
//   bun run reviewer/index.ts
import { agentName, correlationId, loadRun, relayKey, type RelayMeta } from "../shared/config"
import { joinAs, joinChannel, readMeta, relay, send } from "../shared/relay"
import { SECTIONS } from "../worker/data"

const run = loadRun()
const me = agentName(run.runId, "reviewer")
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), "reviewer", ...a)
const short = (name: string) => name.replace(`${run.runId}-`, "")

// Checkpoints seen per task and attempt, so each takeover is reviewed once.
const reviewed = new Set<string>()
const firstAuthor = new Map<string, string>()

async function main() {
    const r = relay()
    const client = await joinAs(r, me)
    await joinChannel(client, run.channel)
    log(`up: reviewing takeovers on #${run.channel}`)
    r.addListener("message.created", async (event: any) => {
        const m = event.message
        if (m?.channel?.name !== run.channel || !String(m.text).startsWith("CKPT ")) return
        const meta = (await readMeta(client, m.id)) as (RelayMeta & { callbacks?: string[]; worker?: string }) | null
        if (!meta || meta.runId !== run.runId || meta.type !== "CKPT" || !meta.stepId) return
        const { taskId, attempt, stepId } = meta
        const author = meta.worker ?? meta.from
        if (attempt === 1) {
            if (!firstAuthor.has(taskId)) firstAuthor.set(taskId, author)
            return
        }
        const key = `${taskId}:${attempt}`
        if (reviewed.has(key)) return
        reviewed.add(key)
        const before = firstAuthor.get(taskId) ?? "the previous agent"
        const callbacks = meta.callbacks ?? []
        const continuity = callbacks.length ? `continuity ok: reuses ${callbacks.join(", ")}` : "continuity weak: no earlier details reused"
        const review = await client
            .reply({
                messageId: m.id,
                text: `REVIEW ${taskId} step ${stepId} (${SECTIONS[stepId - 1]}), first by ${short(author)} after a takeover: ${continuity}.`,
                metadata: { v: 1, type: "REVIEW", runId: run.runId, taskId, attempt, stepId, from: me, correlationId: correlationId(run.runId, taskId, attempt) },
            })
            .catch(e => {
                log("review reply failed", e?.message)
                return null
            })
        const note = callbacks.length
            ? `Keep ${short(before)}'s voice for the rest of the report and keep tying back to ${callbacks[0]}.`
            : `Tie each remaining section back to a detail from ${short(before)}'s sections.`
        const directiveId = `r${Date.now().toString(36)}`
        const steer: RelayMeta & { replyTo?: string } = { v: 1, type: "STEER", runId: run.runId, taskId, attempt, directiveId, from: me, correlationId: correlationId(run.runId, taskId, attempt), replyTo: review ? m.id : undefined }
        await send(client, `@${author}`, `STEER ${taskId}: Reviewer note: ${note}`, steer, relayKey(run.runId, taskId, attempt, `review-steer:${stepId}`)).catch(e => log("steer send failed", e?.message))
        log(`reviewed ${taskId} step ${stepId} by ${short(author)}; sent a note`)
    })
}

main().catch(e => {
    log("fatal", e?.message ?? e)
    process.exit(1)
})

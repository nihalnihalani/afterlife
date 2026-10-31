import { Actor, type ActorSocket, Emittable, Ephemeral, Persisted } from "durable-actors"

// One long-running agent task. The actor owns the lease (its single alarm), the attempt
// counter used as a fencing token, and the ledger of completed steps. When the lease runs
// out, the actor itself offers the task to the standby agents connected to it; the first
// claim wins. No supervisor process is involved. All LLM and Relay I/O happens in agents.
// Arrays are always reassigned: push() on a persisted array is not saved.

export type Status = "queued" | "running" | "lost" | "done" | "failed"

export type Checkpoint = {
    stepId: number
    attempt: number
    worker: string
    summary: string
    text: string
    callbacks: string[]
    at: number
    // Set when this step finished a sentence that a dead agent was cut off in the middle of.
    continued?: { from: string; chars: number }
}

// The step an agent is writing right now, streamed in while it writes.
export type Draft = { stepId: number; attempt: number; worker: string; text: string; at: number }
export type Directive = { id: string; text: string; from: string; at: number; worker: string; attempt: number }
export type Rejection = { at: number; worker: string; op: string; seenAttempt: number; currentAttempt: number; reason: string }
export type TimelineEntry = { at: number; kind: string; detail: string }
export type Takeover = { attempt: number; from: string | null; to: string; atStep: number; at: number; afterMs: number | null }
export type Fail = { ok: false; reason: string }

type Viewer = { viewer: string; role?: "board" | "standby"; name?: string }
type Incoming = { type: "ping" }
type Offer = { type: "offer"; taskId: string; attempt: number; lostWorker: string | null; nextStep: number }
type Socket = ActorSocket<Viewer, Offer>

const SUMMARY_MAX = 240
const TEXT_MAX = 1600
const DIRECTIVE_MAX = 200
const LEASE_MIN_MS = 3000
const LEASE_MAX_MS = 180000

export class Task extends Actor<Viewer, Incoming, Offer> {
    @Persisted @Emittable taskId = ""
    @Persisted @Emittable title = ""
    @Persisted @Emittable totalSteps = 6
    @Persisted @Emittable status: Status = "queued"
    @Persisted @Emittable attempt = 0
    @Persisted @Emittable assignee: string | null = null
    @Persisted @Emittable lostWorker: string | null = null
    @Persisted @Emittable leaseMs = 10000
    // Lease length of the current attempt. An agent may ask for a longer one when it claims
    // (a Claude Code session writes a section in tens of seconds, not two).
    @Persisted @Emittable activeLeaseMs = 10000
    @Persisted @Emittable leaseDeadline: number | null = null
    @Persisted @Emittable lastBeatAt: number | null = null
    @Persisted @Emittable ledger: Checkpoint[] = []
    @Persisted @Emittable draft: Draft | null = null
    @Persisted @Emittable cutOff: Draft | null = null
    @Persisted @Emittable takeover: Takeover | null = null
    @Persisted @Emittable rejections: Rejection[] = []
    @Persisted @Emittable timeline: TimelineEntry[] = []
    @Persisted @Emittable directives: Directive[] = []
    @Persisted @Emittable lostAt: number | null = null
    @Persisted maxAttempts = 3
    @Ephemeral private bootedAt = Date.now()

    async init(input: { taskId: string; title: string; totalSteps: number; leaseMs?: number; maxAttempts?: number }) {
        if (this.taskId) return { ok: true as const, existing: true }
        this.taskId = input.taskId
        this.title = input.title
        this.totalSteps = input.totalSteps
        if (input.leaseMs) this.leaseMs = this.activeLeaseMs = input.leaseMs
        if (input.maxAttempts) this.maxAttempts = input.maxAttempts
        await this.log("init", `${input.title} (${input.totalSteps} steps)`)
        return { ok: true as const, existing: false }
    }

    // Attempt 1 is claimed from `queued`. Every later attempt is claimed from `lost` by
    // whichever agent asks first; the attempt number makes a second claim fail.
    async claim(input: { worker: string; attempt: number; leaseMs?: number }) {
        const { worker, attempt } = input
        if (this.status === "running" && worker === this.assignee && attempt === this.attempt) {
            return { ok: true as const, already: true, ...(await this.resumeInfo()) }
        }
        if (this.status === "queued") {
            if (attempt !== 1) return await this.reject(worker, "claim", attempt, "stale-attempt")
            this.attempt = 1
        } else if (this.status === "lost") {
            if (attempt !== this.attempt) return await this.reject(worker, "claim", attempt, "stale-attempt")
            if (worker === this.lostWorker) return await this.reject(worker, "claim", attempt, "was-lost")
            const now = Date.now()
            this.takeover = { attempt, from: this.lostWorker, to: worker, atStep: this.ledger.length + 1, at: now, afterMs: this.lostAt ? now - this.lostAt : null }
        } else {
            return { ok: false as const, reason: `not-claimable-${this.status}` }
        }
        this.assignee = worker
        this.status = "running"
        this.activeLeaseMs = Math.min(LEASE_MAX_MS, Math.max(LEASE_MIN_MS, input.leaseMs ?? this.leaseMs))
        await this.renewLease()
        const from = this.takeover?.attempt === this.attempt ? ` from ${this.takeover.from ?? "nobody"}` : ""
        await this.log("claim", `${worker} owns attempt ${this.attempt}${from}, resumes at step ${this.ledger.length + 1}`)
        return { ok: true as const, already: false, ...(await this.resumeInfo()) }
    }

    async heartbeat(input: { worker: string; attempt: number }) {
        const fail = await this.fence(input.worker, input.attempt, "heartbeat")
        if (fail) return fail
        await this.renewLease()
        return { ok: true as const, leaseDeadline: this.leaseDeadline }
    }

    // Partial text of the step being written. Visible live on the board, and kept as
    // `cutOff` if the writer dies, so the next agent can finish the sentence.
    async writeDraft(input: { worker: string; attempt: number; stepId: number; text: string }) {
        const fail = await this.fence(input.worker, input.attempt, "draft")
        if (fail) return fail
        if (input.stepId !== this.ledger.length + 1) return { ok: false as const, reason: "out-of-order" }
        this.draft = { stepId: input.stepId, attempt: input.attempt, worker: input.worker, text: input.text.slice(0, TEXT_MAX), at: Date.now() }
        await this.renewLease()
        return { ok: true as const }
    }

    async checkpoint(input: { worker: string; attempt: number; step: { stepId: number; summary: string; text: string; callbacks: string[] } }) {
        const { worker, attempt, step } = input
        const fail = await this.fence(worker, attempt, "checkpoint")
        if (fail) return fail
        if (step.stepId <= this.ledger.length) {
            await this.renewLease()
            return { ok: true as const, duplicate: true, stored: this.ledger[step.stepId - 1], nextStep: this.ledger.length + 1 }
        }
        if (step.stepId !== this.ledger.length + 1) return { ok: false as const, reason: "out-of-order" }
        const stored: Checkpoint = {
            stepId: step.stepId,
            attempt,
            worker,
            summary: step.summary.slice(0, SUMMARY_MAX),
            text: step.text.slice(0, TEXT_MAX),
            callbacks: step.callbacks.slice(0, 6),
            at: Date.now(),
        }
        if (this.cutOff && this.cutOff.stepId === step.stepId) {
            if (stored.text.startsWith(this.cutOff.text)) stored.continued = { from: this.cutOff.worker, chars: this.cutOff.text.length }
            this.cutOff = null
        }
        this.ledger = [...this.ledger, stored]
        this.draft = null
        await this.renewLease()
        await this.log("checkpoint", `step ${step.stepId}/${this.totalSteps} by ${worker}${stored.continued ? ` (finished ${stored.continued.from}'s cut-off sentence)` : ""}`)
        return { ok: true as const, duplicate: false, stored, nextStep: this.ledger.length + 1 }
    }

    async addDirective(input: { worker: string; attempt: number; directive: { id: string; text: string; from: string; at: number } }) {
        const { worker, attempt, directive } = input
        const fail = await this.fence(worker, attempt, "addDirective")
        if (fail) return fail
        if (this.directives.some(d => d.id === directive.id)) return { ok: true as const, duplicate: true }
        const d: Directive = { ...directive, text: directive.text.slice(0, DIRECTIVE_MAX), worker, attempt }
        this.directives = [...this.directives, d].slice(-10)
        await this.log("directive", `${directive.from}: "${d.text}"`)
        return { ok: true as const, duplicate: false }
    }

    async complete(input: { worker: string; attempt: number }) {
        if (this.status === "done" && input.worker === this.assignee && input.attempt === this.attempt) return { ok: true as const }
        const fail = await this.fence(input.worker, input.attempt, "complete")
        if (fail) return fail
        if (this.ledger.length !== this.totalSteps) return { ok: false as const, reason: "incomplete" }
        this.status = "done"
        this.leaseDeadline = null
        this.draft = null
        this.alarm.delete()
        await this.log("done", `completed by ${input.worker} on attempt ${this.attempt}`)
        return { ok: true as const }
    }

    // Safety net for a lost alarm (G1: a runtime crash dropped a pending alarm).
    // Anyone may call it; it only acts on a lease that has really expired.
    async expire(input: { attempt: number }) {
        if (input.attempt !== this.attempt) return { ok: false as const, reason: "stale-attempt" }
        if (this.status !== "running" || this.leaseDeadline === null || Date.now() < this.leaseDeadline) {
            return { ok: false as const, reason: "not-expired" }
        }
        await this.declareLost("lease expired (alarm missed, detected by a caller)")
        return { ok: true as const }
    }

    // Re-arm the alarm from the persisted deadline, e.g. after an actor-runtime restart.
    async rearm() {
        if (this.status !== "running" || this.leaseDeadline === null) return { ok: false as const, reason: "not-leased" }
        this.alarm.set(this.leaseDeadline)
        return { ok: true as const, leaseDeadline: this.leaseDeadline }
    }

    async getState() {
        return {
            taskId: this.taskId,
            title: this.title,
            totalSteps: this.totalSteps,
            status: this.status,
            attempt: this.attempt,
            assignee: this.assignee,
            lostWorker: this.lostWorker,
            leaseDeadline: this.leaseDeadline,
            activeLeaseMs: this.activeLeaseMs,
            ledgerLength: this.ledger.length,
            directives: this.directives,
            cutOff: this.cutOff,
            takeover: this.takeover,
            bootedAt: this.bootedAt,
            alarmAt: this.alarm.get(),
            now: Date.now(),
        }
    }

    async onAlarm() {
        if (this.status !== "running" || this.leaseDeadline === null) return
        if (Date.now() < this.leaseDeadline - 250) {
            this.alarm.set(this.leaseDeadline)
            return
        }
        await this.declareLost("lease expired")
    }

    async onConnect(socket: Socket) {
        const standby = socket.metadata?.role === "standby"
        socket.setTags(standby ? "standby" : "board")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        // A standby that connects (or reconnects) while the task is waiting gets the offer at once.
        if (standby && this.status === "lost") socket.send(await this.offer())
    }

    async onMessage(_socket: Socket, _message: Incoming) {}

    private async declareLost(reason: string) {
        this.lostWorker = this.assignee
        this.assignee = null
        this.attempt = this.attempt + 1
        this.lostAt = Date.now()
        this.leaseDeadline = null
        // The half-written step of the dead agent survives as `cutOff` for its successor.
        if (this.draft && this.draft.stepId === this.ledger.length + 1) this.cutOff = this.draft
        this.draft = null
        if (this.attempt > this.maxAttempts) {
            this.status = "failed"
            await this.log("failed", `${this.lostWorker ?? "nobody"} lost; ${this.maxAttempts} attempts used, needs a human`)
            return
        }
        this.status = "lost"
        const cut = this.cutOff ? `, cut off mid-step ${this.cutOff.stepId}` : ""
        await this.log("lost", `${this.lostWorker ?? "nobody"}: ${reason}${cut}; offered attempt ${this.attempt} to standbys`)
        this.broadcast(await this.offer(), { tags: ["standby"] })
    }

    private async offer(): Promise<Offer> {
        return { type: "offer", taskId: this.taskId, attempt: this.attempt, lostWorker: this.lostWorker, nextStep: this.ledger.length + 1 }
    }

    private async resumeInfo() {
        return { attempt: this.attempt, ledger: this.ledger, directives: this.directives, cutOff: this.cutOff, nextStep: this.ledger.length + 1, leaseMs: this.activeLeaseMs }
    }

    private async renewLease() {
        const now = Date.now()
        this.lastBeatAt = now
        this.leaseDeadline = now + this.activeLeaseMs
        this.alarm.set(this.leaseDeadline)
    }

    // Fencing order: stale attempt, then wrong owner, then not running. Stale and
    // wrong-owner writes are recorded and returned, never thrown, so the evidence survives.
    private async fence(worker: string, attempt: number, op: string): Promise<Fail | null> {
        if (attempt !== this.attempt) return await this.reject(worker, op, attempt, "stale-attempt")
        if (worker !== this.assignee) return await this.reject(worker, op, attempt, "wrong-owner")
        if (this.status !== "running") return { ok: false, reason: "not-running" }
        return null
    }

    private async reject(worker: string, op: string, seenAttempt: number, reason: string): Promise<Fail> {
        const r: Rejection = { at: Date.now(), worker, op, seenAttempt, currentAttempt: this.attempt, reason }
        this.rejections = [...this.rejections, r].slice(-20)
        await this.log("rejected", `${op} from ${worker}: ${reason} (sent attempt ${seenAttempt}, current ${this.attempt})`)
        return { ok: false, reason }
    }

    private async log(kind: string, detail: string) {
        this.timeline = [...this.timeline, { at: Date.now(), kind, detail }].slice(-40)
    }
}

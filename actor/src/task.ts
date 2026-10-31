import { Actor, type ActorSocket, Emittable, Ephemeral, Persisted } from "durable-actors"

// One long-running agent task. The actor owns the lease (its single alarm), the attempt
// counter used as a fencing token, and the ledger of completed steps. All LLM and Relay
// I/O happens in workers and the bridge, so every method here is short.
// Arrays are always reassigned: push() on a persisted array is not saved.

export type Status = "queued" | "running" | "lost" | "handing-off" | "done" | "failed"

export type Checkpoint = {
    stepId: number
    attempt: number
    worker: string
    summary: string
    text: string
    callbacks: string[]
    at: number
}

export type Directive = { id: string; text: string; from: string; at: number; worker: string; attempt: number }
export type Rejection = { at: number; worker: string; op: string; seenAttempt: number; currentAttempt: number; reason: string }
export type TimelineEntry = { at: number; kind: string; detail: string }
export type Handoff = { attempt: number; to: string; dmSent: boolean }
export type Fail = { ok: false; reason: string }

type Viewer = { viewer: string }
type Incoming = { type: "ping" }
type Socket = ActorSocket<Viewer, never>

const SUMMARY_MAX = 240
const TEXT_MAX = 1200
const DIRECTIVE_MAX = 200

export class Task extends Actor<Viewer, Incoming, never> {
    @Persisted @Emittable taskId = ""
    @Persisted @Emittable title = ""
    @Persisted @Emittable totalSteps = 6
    @Persisted @Emittable status: Status = "queued"
    @Persisted @Emittable attempt = 0
    @Persisted @Emittable assignee: string | null = null
    @Persisted @Emittable lostWorker: string | null = null
    @Persisted @Emittable leaseMs = 10000
    @Persisted @Emittable leaseDeadline: number | null = null
    @Persisted @Emittable lastBeatAt: number | null = null
    @Persisted @Emittable ledger: Checkpoint[] = []
    @Persisted @Emittable handoff: Handoff | null = null
    @Persisted @Emittable rejections: Rejection[] = []
    @Persisted @Emittable timeline: TimelineEntry[] = []
    @Persisted @Emittable directives: Directive[] = []
    @Persisted @Emittable lostAt: number | null = null
    @Persisted handoffTimeoutMs = 15000
    @Persisted maxAttempts = 3
    @Ephemeral private bootedAt = Date.now()

    async init(input: { taskId: string; title: string; totalSteps: number; leaseMs?: number; handoffTimeoutMs?: number; maxAttempts?: number }) {
        if (this.taskId) return { ok: true as const, existing: true }
        this.taskId = input.taskId
        this.title = input.title
        this.totalSteps = input.totalSteps
        if (input.leaseMs) this.leaseMs = input.leaseMs
        if (input.handoffTimeoutMs) this.handoffTimeoutMs = input.handoffTimeoutMs
        if (input.maxAttempts) this.maxAttempts = input.maxAttempts
        await this.log("init", `${input.title} (${input.totalSteps} steps)`)
        return { ok: true as const, existing: false }
    }

    async claim(input: { worker: string; attempt: number }) {
        const { worker, attempt } = input
        if (this.status === "running" && worker === this.assignee && attempt === this.attempt) {
            return { ok: true as const, already: true, attempt, ledger: this.ledger, directives: this.directives, nextStep: this.ledger.length + 1 }
        }
        if (this.status === "queued") {
            if (attempt !== 1) return await this.reject(worker, "claim", attempt, "stale-attempt")
            this.attempt = 1
        } else if (this.status === "handing-off") {
            if (attempt !== this.attempt) return await this.reject(worker, "claim", attempt, "stale-attempt")
            if (worker !== this.assignee) return await this.reject(worker, "claim", attempt, "wrong-owner")
            if (this.handoff) this.handoff = { ...this.handoff, dmSent: true }
        } else {
            return await this.reject(worker, "claim", attempt, "not-claimable")
        }
        this.assignee = worker
        this.status = "running"
        await this.renewLease()
        await this.log("claim", `${worker} owns attempt ${this.attempt}, resumes at step ${this.ledger.length + 1}`)
        return { ok: true as const, already: false, attempt: this.attempt, ledger: this.ledger, directives: this.directives, nextStep: this.ledger.length + 1 }
    }

    async heartbeat(input: { worker: string; attempt: number }) {
        const fail = await this.fence(input.worker, input.attempt, "heartbeat")
        if (fail) return fail
        await this.renewLease()
        return { ok: true as const, leaseDeadline: this.leaseDeadline }
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
        this.ledger = [...this.ledger, stored]
        await this.renewLease()
        await this.log("checkpoint", `step ${step.stepId}/${this.totalSteps} by ${worker}`)
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
        this.alarm.delete()
        await this.log("done", `completed by ${input.worker} on attempt ${this.attempt}`)
        return { ok: true as const }
    }

    async beginHandoff(input: { standby: string; attempt: number }) {
        const { standby, attempt } = input
        if (this.status === "handing-off" && this.handoff && this.handoff.to === standby && this.handoff.attempt === attempt) {
            return { ok: true as const, packet: await this.packet() }
        }
        if (this.status !== "lost") return { ok: false as const, reason: `status-${this.status}` }
        if (attempt !== this.attempt) return { ok: false as const, reason: "stale-attempt" }
        this.status = "handing-off"
        this.assignee = standby
        this.handoff = { attempt, to: standby, dmSent: false }
        this.leaseDeadline = Date.now() + this.handoffTimeoutMs
        this.alarm.set(this.leaseDeadline)
        await this.log("handoff", `attempt ${attempt} offered to ${standby}`)
        return { ok: true as const, packet: await this.packet() }
    }

    async markHandoffSent(input: { attempt: number }) {
        if (this.handoff && this.handoff.attempt === input.attempt) this.handoff = { ...this.handoff, dmSent: true }
        return { ok: true as const }
    }

    // Safety net for a lost alarm (G1: a runtime crash dropped a pending alarm).
    // The bridge calls this when it sees an expired lease that the alarm did not handle.
    async expire(input: { attempt: number }) {
        if (input.attempt !== this.attempt) return { ok: false as const, reason: "stale-attempt" }
        if (!(await this.isLeased()) || this.leaseDeadline === null || Date.now() < this.leaseDeadline) {
            return { ok: false as const, reason: "not-expired" }
        }
        await this.declareLost("lease expired (detected by bridge)")
        return { ok: true as const }
    }

    // Re-arm the alarm from the persisted deadline, e.g. after an actor-runtime restart.
    async rearm() {
        if (!(await this.isLeased()) || this.leaseDeadline === null) return { ok: false as const, reason: "not-leased" }
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
            handoff: this.handoff,
            ledgerLength: this.ledger.length,
            directives: this.directives,
            bootedAt: this.bootedAt,
            alarmAt: this.alarm.get(),
            now: Date.now(),
        }
    }

    async onAlarm() {
        if (!(await this.isLeased()) || this.leaseDeadline === null) return
        if (Date.now() < this.leaseDeadline - 250) {
            this.alarm.set(this.leaseDeadline)
            return
        }
        await this.declareLost(this.status === "handing-off" ? "standby never claimed" : "lease expired")
    }

    async onConnect(socket: Socket) {
        socket.setTags("board")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
    }

    async onMessage(_socket: Socket, _message: Incoming) {}

    private async isLeased() {
        return this.status === "running" || this.status === "handing-off"
    }

    private async declareLost(reason: string) {
        this.lostWorker = this.assignee
        this.assignee = null
        this.attempt = this.attempt + 1
        this.lostAt = Date.now()
        this.leaseDeadline = null
        this.handoff = null
        if (this.attempt > this.maxAttempts) {
            this.status = "failed"
            await this.log("failed", `${this.lostWorker ?? "nobody"} lost; ${this.maxAttempts} attempts used, needs a human`)
        } else {
            this.status = "lost"
            await this.log("lost", `${this.lostWorker ?? "nobody"}: ${reason}; now attempt ${this.attempt}`)
        }
    }

    private async renewLease() {
        const now = Date.now()
        this.lastBeatAt = now
        this.leaseDeadline = now + this.leaseMs
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

    private async packet() {
        return { taskId: this.taskId, title: this.title, attempt: this.attempt, ledger: this.ledger, directives: this.directives }
    }

    private async log(kind: string, detail: string) {
        this.timeline = [...this.timeline, { at: Date.now(), kind, detail }].slice(-40)
    }
}

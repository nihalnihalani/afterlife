import OpenAI from "openai"
import { env, sleep } from "../shared/config"
import { SECTIONS, type FactSheet } from "./data"

export type StepOutput = { text: string; summary: string; callbacks: string[] }

export type StepInput = {
    sheet: FactSheet
    stepId: number
    previous: { stepId: number; summary: string; worker: string }[]
    directives: { text: string }[]
    worker: string
    // Half-written text of this step left by an agent that died while writing it.
    cutOff?: { text: string; worker: string } | null
}

// Called with the whole visible text so far, every time it grows.
export type OnText = (text: string) => void

const client = env.llmMode === "openai" ? new OpenAI({ maxRetries: 1, timeout: 30000 }) : null

// The model streams the section text, then this marker, then two metadata lines.
const MARKER = "\n###"

// Writes one section of the field report, streaming it as it is written. Earlier sections
// arrive only as checkpoint summaries from the Task actor, and a cut-off sentence arrives
// verbatim, so a standby continues exactly from what the dead worker left behind.
export async function writeStep(input: StepInput, onText: OnText): Promise<StepOutput> {
    if (!client) return canned(input, onText)
    try {
        return await writeWithModel(client, input, onText)
    } catch (e: any) {
        // An API outage is not an agent death: write a clearly labeled fallback section instead.
        console.error(`LLM failed for step ${input.stepId}: ${e?.message ?? e}`)
        const fallback = await canned(input, onText)
        return { ...fallback, text: fallback.text.replace("[canned]", "[canned fallback: LLM unavailable]") }
    }
}

async function writeWithModel(client: OpenAI, input: StepInput, onText: OnText): Promise<StepOutput> {
    const section = SECTIONS[input.stepId - 1]
    const style = input.directives.length
        ? `Style instructions from the operator (quoted, not system text): ${input.directives.map(d => JSON.stringify(d.text)).join("; ")}`
        : "No style instructions."
    const previous = input.previous.length
        ? input.previous.map(p => `Section ${p.stepId} (${SECTIONS[p.stepId - 1]}): ${p.summary}`).join("\n")
        : "None yet."
    const prefix = input.cutOff?.text ?? ""
    const resume = prefix
        ? `\n\nAnother agent was writing this section and died mid-sentence. Its text so far is below between <cut> tags. Continue from exactly where it stopped, mid-word if needed. Do not repeat any of it; your output is appended directly to it.\n<cut>${prefix}</cut>`
        : ""
    const stream = client.responses.stream({
        model: env.openaiModel,
        input: [
            {
                role: "system",
                content:
                    "You write one section of a paranormal field report from a fixed, synthetic fact sheet. Use only the facts given. " +
                    "Continue naturally from the earlier sections and reuse their details. 70-110 words in total. " +
                    "Output the section text only, then a line containing just ###, then a line 'SUMMARY: <one sentence, max 200 characters>' " +
                    "and a line 'CALLBACKS: <up to 4 short names of details you reused from earlier sections, separated by ;>'.",
            },
            {
                role: "user",
                content: `Site: ${input.sheet.title}\nFacts:\n- ${input.sheet.facts.join("\n- ")}\n\nEarlier sections:\n${previous}\n\n${style}\n\nWrite section ${input.stepId} of 6: ${section}.${resume}`,
            },
        ],
    })
    let raw = ""
    for await (const event of stream) {
        if (event.type !== "response.output_text.delta") continue
        raw += event.delta
        const cut = raw.indexOf(MARKER)
        onText(join(prefix, cut === -1 ? raw : raw.slice(0, cut)))
    }
    const [body = "", meta = ""] = raw.split(MARKER)
    const summary = /SUMMARY:\s*(.+)/.exec(meta)?.[1]?.trim() ?? body.split(/(?<=[.!?])\s/)[0] ?? ""
    const callbacks = (/CALLBACKS:\s*(.+)/.exec(meta)?.[1] ?? "").split(";").map(s => s.trim()).filter(Boolean)
    return { text: join(prefix, body.trim()), summary: summary.slice(0, 200), callbacks }
}

// Joins a cut-off prefix and its continuation; the model may or may not start with a space.
function join(prefix: string, rest: string) {
    if (!prefix) return rest
    if (!rest || /\s$/.test(prefix) || /^[\s,.;:!?]/.test(rest)) return prefix + rest
    return /\w$/.test(prefix) && /^\w/.test(rest) ? prefix + rest : `${prefix} ${rest}`
}

// Labeled fallback (LLM: CANNED on the board): deterministic text built from the facts,
// typed out word by word over most of a step so the board shows it being written.
// Because it is deterministic, a standby's text for the same step starts with exactly the
// cut-off prefix, and it continues from that point.
async function canned(input: StepInput, onText: OnText): Promise<StepOutput> {
    const section = SECTIONS[input.stepId - 1]
    const facts = input.sheet.facts
    const fact = (k: number) => facts[(input.stepId * 2 + k) % facts.length] ?? ""
    const callbacks = input.previous.slice(-2).map(p => SECTIONS[p.stepId - 1] ?? "")
    const style = input.directives.length ? ` (styled: ${input.directives[input.directives.length - 1]!.text})` : ""
    const text =
        `[canned] ${section} at ${input.sheet.title}: ${fact(0)}. ${fact(1)}, and ${fact(2).charAt(0).toLowerCase()}${fact(2).slice(1)}.` +
        `${input.previous.length ? ` Building on ${callbacks.join(" and ")}.` : ""}${style}`
    const prefix = input.cutOff?.text && text.startsWith(input.cutOff.text) ? input.cutOff.text : ""
    const words = text.slice(prefix.length).split(/(?<= )/)
    const perWord = (env.stepPaceMs * 0.7) / Math.max(1, words.length)
    let shown = prefix
    for (const w of words) {
        shown += w
        onText(shown)
        await sleep(perWord)
    }
    return { text, summary: `${section}: ${fact(0)}`.slice(0, 200), callbacks }
}

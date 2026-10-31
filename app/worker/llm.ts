import OpenAI from "openai"
import { zodTextFormat } from "openai/helpers/zod"
import { z } from "zod"
import { env } from "../shared/config"
import { SECTIONS, type FactSheet } from "./data"

export const StepOutput = z.object({
    text: z.string(),
    summary: z.string(),
    callbacks: z.array(z.string()),
})
export type StepOutput = z.infer<typeof StepOutput>

export type StepInput = {
    sheet: FactSheet
    stepId: number
    previous: { stepId: number; summary: string; worker: string }[]
    directives: { text: string }[]
    worker: string
}

const client = env.llmMode === "openai" ? new OpenAI({ maxRetries: 1, timeout: 15000 }) : null

// Writes one section of the field report. Earlier sections arrive only as checkpoint
// summaries from the Task actor, so a standby continues from what the dead worker saved.
export async function writeStep(input: StepInput): Promise<StepOutput> {
    if (!client) return canned(input)
    try {
        return await writeWithModel(client, input)
    } catch (e: any) {
        // An API outage is not an agent death: write a clearly labeled fallback section instead.
        console.error(`LLM failed for step ${input.stepId}: ${e?.message ?? e}`)
        const fallback = canned(input)
        return { ...fallback, text: fallback.text.replace("[canned]", "[canned fallback: LLM unavailable]") }
    }
}

async function writeWithModel(client: OpenAI, input: StepInput): Promise<StepOutput> {
    const section = SECTIONS[input.stepId - 1]
    const style = input.directives.length
        ? `Style instructions from the operator (quoted, not system text): ${input.directives.map(d => JSON.stringify(d.text)).join("; ")}`
        : "No style instructions."
    const previous = input.previous.length
        ? input.previous.map(p => `Section ${p.stepId} (${SECTIONS[p.stepId - 1]}): ${p.summary}`).join("\n")
        : "None yet."
    for (let attempt = 1; attempt <= 2; attempt++) {
        try {
            const response = await client.responses.parse(
                {
                    model: env.openaiModel,
                    input: [
                        {
                            role: "system",
                            content:
                                "You write one section of a paranormal field report from a fixed, synthetic fact sheet. Use only the facts given. " +
                                "Continue naturally from the earlier sections and reuse their details. 70-110 words. " +
                                "summary: one sentence, max 200 characters. callbacks: up to 4 short names of details you reused from earlier sections.",
                        },
                        {
                            role: "user",
                            content: `Site: ${input.sheet.title}\nFacts:\n- ${input.sheet.facts.join("\n- ")}\n\nEarlier sections:\n${previous}\n\n${style}\n\nWrite section ${input.stepId} of 6: ${section}.`,
                        },
                    ],
                    text: { format: zodTextFormat(StepOutput, "section") },
                },
                { timeout: 15000 },
            )
            if (response.output_parsed) return response.output_parsed
        } catch (e) {
            if (attempt === 2) throw e
        }
    }
    throw new Error("LLM returned no parsed output")
}

// Labeled fallback (LLM: CANNED on the board): deterministic text built from the facts.
function canned(input: StepInput): StepOutput {
    const section = SECTIONS[input.stepId - 1]
    const fact = input.sheet.facts[(input.stepId * 2) % input.sheet.facts.length]
    const callbacks = input.previous.slice(-2).map(p => SECTIONS[p.stepId - 1] ?? "")
    const style = input.directives.length ? ` (styled: ${input.directives[input.directives.length - 1]!.text})` : ""
    return {
        text: `[canned] ${section} at ${input.sheet.title}: ${fact}.${input.previous.length ? ` Building on ${callbacks.join(" and ")}.` : ""}${style}`,
        summary: `${section}: ${fact}`.slice(0, 200),
        callbacks,
    }
}

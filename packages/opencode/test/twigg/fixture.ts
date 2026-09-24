import { TwiggModels } from "../../src/twigg/models"

export const slot = (mime_types: string[]) => ({
  mime_types,
  max_bytes_per_attachment: 1000,
  max_attachments_per_request: null,
})

export function catalogueModel(input: Partial<TwiggModels.Model> & { name: string }): TwiggModels.Model {
  return {
    display_name: input.name.toUpperCase(),
    model_family: "family",
    context_window: 1_000_000,
    max_output_tokens: 128_000,
    supports_reasoning: true,
    supports_tools: true,
    max_reasoning_effort: null,
    max_tool_definitions: null,
    media: { max_attachments_per_part: 20, user: slot(["image/png", "application/pdf"]), tool_result: slot([]) },
    rates: { input: "10.5", output: "52.5", cache_read: "0.2625", cache_write: "13.125" },
    tiers: [],
    ...input,
  }
}

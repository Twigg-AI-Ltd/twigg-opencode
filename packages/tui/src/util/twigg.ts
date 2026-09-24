// Twigg holds a twigg session's conversation. Features that rewrite local history, or add to it without a model call,
// would make it drift from the Twigg chat, so they stay off until Twigg can fork chats
// (TODO(twigg-api#2), context/twigg/api-requests.md).
export const COMING_SOON = {
  fork: "Forking is coming soon for Twigg sessions",
  undo: "Undo and redo are coming soon for Twigg sessions",
  share: "Sharing is coming soon for Twigg sessions",
  shell: "Shell commands aren't available in Twigg sessions yet",
} as const

export function twiggUnsupported(input: { session?: { metadata?: Record<string, unknown> }; providerID?: string }) {
  return input.session?.metadata?.twigg !== undefined || input.providerID === "twigg"
}

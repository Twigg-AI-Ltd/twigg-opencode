// Shared provider config for tests that need opencode to talk to a fake model over a real HTTP endpoint. It connects
// the `twigg` provider to the URL the caller supplies (typically a TestLLMServer instance, which speaks the Twigg
// protocol), and the fake's catalogue has a single model, `test-model` (i.e. `--model twigg/test-model`).
//
// Used by:
//   - test/lib/run-process.ts          (subprocess CLI tests)
//   - test/server/httpapi-sdk.test.ts  (in-process SDK tests)
export const TEST_MODEL = "twigg/test-model"

export function testProviderConfig(llmUrl: string) {
  return {
    formatter: false,
    lsp: false,
    twigg: { baseURL: llmUrl },
    provider: {
      twigg: {
        options: { apiKey: "test-key" },
      },
    },
  }
}

// The pre-Twigg `test` provider (AI SDK, OpenAI-compatible). Only for tests of AI SDK behaviour, which go away with
// the AI SDK path (Phase 7).
export function legacyTestProviderConfig(llmUrl: string) {
  return {
    formatter: false,
    lsp: false,
    provider: {
      test: {
        name: "Test",
        id: "test",
        env: [],
        npm: "@ai-sdk/openai-compatible",
        models: {
          "test-model": {
            id: "test-model",
            name: "Test Model",
            attachment: false,
            reasoning: false,
            temperature: false,
            tool_call: true,
            release_date: "2025-01-01",
            limit: { context: 100_000, output: 10_000 },
            cost: { input: 0, output: 0 },
            options: {},
          },
        },
        options: { apiKey: "test-key", baseURL: llmUrl },
      },
    },
  }
}

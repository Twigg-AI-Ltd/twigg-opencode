# Session LLM Runtime Boundaries

`../llm.ts` is the opencode session LLM service. It owns opencode concerns: config, provider resolution, plugins,
instructions and the Twigg chat link. Every model runs through Twigg (`src/twigg/runtime.ts`), which keeps the
conversation server-side. The AI SDK and native runtimes were removed in the Twigg port (Phase 7).

- `request.ts` (`LLMRequestPrep.prepare`) builds the system text, filters tools by permission and runs the
  `chat.params` and `chat.headers` plugin hooks. The Twigg runtime sends the filtered tools and max tokens; the
  system text is split into published namespace instructions and a per-turn context block in `../llm.ts`.
- Tool execution stays opencode-owned: the Twigg runtime runs the pending tool calls through each AI SDK `Tool`'s
  `execute` and emits `tool-result` / `tool-error` events.
- Everything downstream consumes `@opencode-ai/llm` `LLMEvent`s, so the session processor doesn't know about Twigg.

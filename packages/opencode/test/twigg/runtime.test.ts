import { describe, expect, test } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { LLMEvent } from "@opencode-ai/llm"
import { jsonSchema, tool } from "ai"
import { Effect, Layer, Option, Stream } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse, UrlParams } from "effect/unstable/http"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { TwiggClient } from "../../src/twigg/client"
import { TwiggInstructions } from "../../src/twigg/instructions"
import { TwiggModels } from "../../src/twigg/models"
import { TwiggRuntime } from "../../src/twigg/runtime"
import { it } from "../lib/effect"
import { catalogueModel, slot } from "./fixture"

const settings = { baseURL: "https://twigg.test/api/v1", apiKey: "tw_test_key" }
const sessionID = SessionID.make("ses_twigg")
const model = TwiggModels.toModel(
  catalogueModel({
    name: "gpt-6-luna",
    media: { max_attachments_per_part: 20, user: slot(["image/png"]), tool_result: slot(["image/png"]) },
  }),
  settings.baseURL,
)
const encoder = new TextEncoder()
const usage = { input_tokens: 10, output_tokens: 5, reasoning_tokens: 2, cache_read_tokens: 3, cache_write_tokens: 0 }

type Route = (request: HttpClientRequest.HttpClientRequest, body: unknown) => Response | undefined

// A scripted Twigg: each route answers once, in order, for its "METHOD /path".
// A scripted Twigg: each route answers once, in order, for its "METHOD /path". Instruction config is a small
// in-memory store, kept out of `requests` and recorded in `config`.
function twigg(
  script: Record<string, Array<Response | Route>>,
  active: Record<string, { mode: string; body: string }> = {},
) {
  const requests: Array<{ key: string; body: unknown }> = []
  const config: string[] = []
  const layer = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        const url = new URL(request.url)
        const key = `${request.method} ${url.pathname.replace("/api/v1", "")}`
        const body =
          request.body._tag === "Uint8Array" ? JSON.parse(new TextDecoder().decode(request.body.body)) : undefined
        if (key.includes("/config/instructions")) {
          // Query parameters live on the request, not in its URL.
          const namespace = Option.getOrElse(UrlParams.getFirst(request.urlParams, "namespace"), () => "")
          return HttpClientResponse.fromWeb(request, instructions(key, namespace, body, active, config))
        }
        requests.push({ key, body })
        const next = script[key]?.shift()
        const response = typeof next === "function" ? next(request, body) : next
        return HttpClientResponse.fromWeb(
          request,
          response ?? Response.json({ error: { code: "not_found", message: key } }, { status: 404 }),
        )
      }),
    ),
  )
  return { layer, requests, config, active }
}

function instructions(
  key: string,
  namespace: string,
  body: unknown,
  active: Record<string, { mode: string; body: string }>,
  log: string[],
) {
  if (key === "GET /config/instructions")
    return Response.json(active[namespace] ? [{ id: namespace, namespace, is_active: true }] : [])
  if (key.startsWith("GET /config/instructions/")) {
    const id = decodeURIComponent(key.slice("GET /config/instructions/".length))
    return Response.json(active[id])
  }
  if (key === "DELETE /config/instructions/active") {
    log.push(`withdraw ${namespace}`)
    delete active[namespace]
    return Response.json({})
  }
  const item = body as { namespace: string; mode: string; body: string }
  log.push(`publish ${item.mode} ${item.namespace}`)
  active[item.namespace] = { mode: item.mode, body: item.body }
  return Response.json({})
}

function sse(frames: Array<[string, unknown]>) {
  const text = frames.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join("")
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(text))
        controller.close()
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  )
}

const run = (id = "run_1"): [string, unknown] => ["run", { run_id: id, chat_id: "chat_1", closed_tool_calls: [] }]
const done = (
  input: { stop_reason?: string; pending?: Array<[string, string]>; cost?: string | null } = {},
): [string, unknown] => [
  "done",
  {
    stop_reason: input.stop_reason ?? "end_turn",
    pending_tool_calls: (input.pending ?? []).map(([tool_use_id, tool_name]) => ({ tool_use_id, tool_name })),
    usage,
    cost: input.cost === undefined ? "0.0012" : input.cost,
    cost_currency: "USD",
    model_served: "gpt-6-luna-2026",
    provider_response_id: null,
    compaction: null,
    server_tools: null,
  },
]
const text = (value: string): Array<[string, unknown]> => [
  ["block_start", { kind: "text" }],
  ["delta", { kind: "text", text: value }],
  ["block_stop", {}],
]
const call = (id: string, name: string, args: string): Array<[string, unknown]> => [
  ["block_start", { kind: "tool_call", tool_name: name, tool_use_id: id }],
  ["delta", { kind: "tool_input", text: args.slice(0, 5) }],
  ["delta", { kind: "tool_input", text: args.slice(5) }],
  ["block_stop", {}],
]

function user(id: string, parts: Array<Record<string, unknown>>): SessionV1.WithParts {
  const messageID = MessageID.make(id)
  return {
    info: { id: messageID, sessionID, role: "user", time: { created: 0 }, agent: "build", model: {} },
    parts: parts.map((part, index) => ({ id: PartID.make(`prt_${id}_${index}`), sessionID, messageID, ...part })),
  } as unknown as SessionV1.WithParts
}

function assistant(id: string, parts: Array<Record<string, unknown>>): SessionV1.WithParts {
  const messageID = MessageID.make(id)
  return {
    info: { id: messageID, sessionID, role: "assistant", time: { created: 0 }, parentID: "msg_1" },
    parts: parts.map((part, index) => ({ id: PartID.make(`prt_${id}_${index}`), sessionID, messageID, ...part })),
  } as unknown as SessionV1.WithParts
}

const completed = (callID: string, output: string, extra: Record<string, unknown> = {}) => ({
  type: "tool",
  tool: "read",
  callID,
  state: { status: "completed", input: {}, output, title: "", metadata: {}, time: { start: 0, end: 1 }, ...extra },
})

function memoryChat(initial?: TwiggRuntime.State) {
  const store = {
    state: initial,
    saves: [] as TwiggRuntime.State[],
    repaired: [] as Array<{ id: string; rows: unknown[] }>,
  }
  const chat: TwiggRuntime.Chat = {
    load: Effect.sync(() => store.state),
    save: (state) =>
      Effect.sync(() => {
        store.state = state
        store.saves.push(state)
      }),
    open: Effect.succeed({ namespace: "twigg-code/test/p/project", user_metadata: { session_id: sessionID } }),
    repair: (id, rows) => Effect.sync(() => void store.repaired.push({ id, rows: [...rows] })),
  }
  return { store, chat }
}

const tools = (log: Array<{ name: string; args: unknown }>) => ({
  read: tool({
    description: "Read a file",
    inputSchema: jsonSchema({ type: "object", properties: { path: { type: "string" } } }),
    execute: async (args) => {
      log.push({ name: "read", args })
      return { output: `contents of ${(args as { path: string }).path}`, title: "read", metadata: {} }
    },
  }),
  fail: tool({
    description: "Always fails",
    inputSchema: jsonSchema({ type: "object", properties: {} }),
    execute: async (): Promise<string> => {
      throw new Error("boom")
    },
  }),
})

function streamInput(
  input: Partial<TwiggRuntime.Input> & Pick<TwiggRuntime.Input, "chat" | "history">,
): TwiggRuntime.Input {
  return {
    settings,
    model,
    assistantID: "msg_a1",
    tools: {},
    messages: [],
    maxTokens: 1000,
    abort: new AbortController().signal,
    instructions: { global: [], project: [] },
    context: "",
    published: new Map(),
    ...input,
  }
}

const collect = (input: TwiggRuntime.Input, layer: Layer.Layer<HttpClient.HttpClient>) =>
  TwiggRuntime.stream(input).pipe(
    Stream.runCollect,
    Effect.map((items) => Array.from(items) as LLMEvent[]),
    Effect.provide(layer),
  )

const failure = (input: TwiggRuntime.Input, layer: Layer.Layer<HttpClient.HttpClient>) =>
  collect(input, layer).pipe(
    Effect.flip,
    Effect.map((error) => error as { name: string; data: { message: string; isRetryable: boolean } }),
  )

describe("twigg runtime", () => {
  it.effect("creates the chat on the first turn and streams a text reply", () =>
    Effect.gen(function* () {
      const server = twigg({
        "POST /chats": [Response.json({ id: "chat_1", namespace: "twigg-code/test/p/project" }, { status: 201 })],
        "POST /chats/chat_1/responses": [sse([run(), ...text("Hello"), done()])],
      })
      const { store, chat } = memoryChat()
      const events = yield* collect(
        streamInput({
          chat,
          history: [user("msg_1", [{ type: "text", text: "  Say\nhello  " }])],
          reasoningEffort: "low",
        }),
        server.layer,
      )

      expect(events.map((event) => event.type)).toEqual([
        "step-start",
        "text-start",
        "text-delta",
        "text-end",
        "step-finish",
        "finish",
      ])
      const finish = events.find((event) => event.type === "step-finish")
      expect(finish).toMatchObject({
        reason: "stop",
        usage: {
          inputTokens: 13,
          nonCachedInputTokens: 10,
          cacheReadInputTokens: 3,
          outputTokens: 5,
          reasoningTokens: 2,
        },
        providerMetadata: { twigg: { cost: "0.0012", run_id: "run_1" } },
      })
      expect(server.requests[0]).toEqual({
        key: "POST /chats",
        body: { namespace: "twigg-code/test/p/project", title: "Say hello", user_metadata: { session_id: sessionID } },
      })
      expect(server.requests[1].body).toEqual({
        model: "gpt-6-luna",
        input: [{ type: "prompt", text: "  Say\nhello  " }],
        idempotency_key: "msg_a1",
        max_tokens: 1000,
        reasoning_effort: "low",
      })
      expect(store.state).toEqual({
        chat_id: "chat_1",
        namespace: "twigg-code/test/p/project",
        cursor_message_id: "msg_1",
      })
      // The run is an orphan between `run` and `done`.
      expect(store.saves.map((state) => state.orphan?.run_id)).toEqual([undefined, "run_1", undefined])
    }),
  )

  it.effect("runs parallel tool calls once done lists them", () =>
    Effect.gen(function* () {
      const server = twigg({
        "POST /chats/chat_1/responses": [
          sse([
            run(),
            ...call("call_1", "read", '{"path":"a.ts"}'),
            ...call("call_2", "fail", "{}"),
            ...call("call_3", "read", "{not json"),
            done({
              stop_reason: "tool_use",
              pending: [
                ["call_1", "read"],
                ["call_2", "fail"],
                ["call_3", "read"],
              ],
            }),
          ]),
        ],
      })
      const log: Array<{ name: string; args: unknown }> = []
      const { chat } = memoryChat({ chat_id: "chat_1", namespace: "ns" })
      const events = yield* collect(
        streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "go" }])], tools: tools(log) }),
        server.layer,
      )

      expect(log).toEqual([{ name: "read", args: { path: "a.ts" } }])
      expect(events.filter((event) => event.type === "tool-call").map((event) => event.id)).toEqual([
        "call_1",
        "call_2",
        "call_3",
      ])
      expect(events.find((event) => event.type === "tool-result")).toMatchObject({
        id: "call_1",
        result: { type: "json", value: { output: "contents of a.ts" } },
      })
      expect(events.filter((event) => event.type === "tool-error").map((event) => [event.id, event.message])).toEqual([
        ["call_2", "boom"],
        ["call_3", "The tool arguments were not valid JSON: {not json"],
      ])
      expect(events.at(-1)).toMatchObject({ type: "finish", reason: "tool-calls" })
      expect(server.requests[0].body).toMatchObject({
        tools: [
          { name: "read", description: "Read a file" },
          { name: "fail", description: "Always fails", input_schema: { type: "object", properties: {} } },
        ],
      })
    }),
  )

  it.effect("fails before the run with retryable and non-retryable errors", () =>
    Effect.gen(function* () {
      const cases: Array<[Response, boolean, string]> = [
        [Response.json({ error: { code: "rate_limited", message: "slow down" } }, { status: 429 }), true, "slow down"],
        [
          Response.json(
            { error: { code: "conflict", message: "chat x is being written by another request" } },
            { status: 409 },
          ),
          true,
          "Waiting for the previous response on this chat to finish",
        ],
        [
          Response.json({ error: { code: "payment_required", message: "no" } }, { status: 402 }),
          false,
          "Your Twigg balance can't cover this request. Top up at https://twigg.ai/dashboard",
        ],
        [
          Response.json({ error: { code: "validation_error", message: "bad input" } }, { status: 422 }),
          false,
          "bad input",
        ],
      ]
      for (const [response, retryable, message] of cases) {
        const server = twigg({ "POST /chats/chat_1/responses": [response] })
        const { store, chat } = memoryChat({ chat_id: "chat_1", namespace: "ns" })
        const error = yield* failure(
          streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "hi" }])] }),
          server.layer,
        )
        expect(error.name).toBe("APIError")
        expect(error.data).toMatchObject({ isRetryable: retryable, message })
        // Nothing landed, so the next request sends the same prompt again.
        expect(store.state?.cursor_message_id).toBeUndefined()
      }
    }),
  )

  it.effect("never retries once the run has started", () =>
    Effect.gen(function* () {
      const server = twigg({
        "POST /chats/chat_1/responses": [
          sse([
            run(),
            ...text("partial"),
            ["error", { code: "external_service_error", message: "provider overloaded" }],
          ]),
        ],
      })
      const { store, chat } = memoryChat({ chat_id: "chat_1", namespace: "ns" })
      const error = yield* failure(
        streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "hi" }])] }),
        server.layer,
      )
      expect(error.data).toMatchObject({ isRetryable: false, message: "provider overloaded" })
      // The run failed on the server, so it is over: the cursor moved and there is no orphan.
      expect(store.state).toEqual({ chat_id: "chat_1", namespace: "ns", cursor_message_id: "msg_1" })
    }),
  )

  it.effect("records an orphan when the stream ends before done", () =>
    Effect.gen(function* () {
      const server = twigg({ "POST /chats/chat_1/responses": [sse([run("run_9"), ...text("partial")])] })
      const { store, chat } = memoryChat({ chat_id: "chat_1", namespace: "ns" })
      const error = yield* failure(
        streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "hi" }])] }),
        server.layer,
      )
      expect(error.data.isRetryable).toBe(false)
      expect(store.state?.orphan).toEqual({ run_id: "run_9", message_id: "msg_a1" })
    }),
  )

  it.live("waits for an orphan, repairs its message, then sends the next turn", () =>
    Effect.gen(function* () {
      const running = { id: "run_9", status: "running", error: null, cost: null, usage: null }
      const rows = [
        { ordinal: 1, role: "user", part: { type: "prompt", text: "hi" } },
        { ordinal: 2, role: "assistant", part: { type: "message", text: "the full answer" } },
      ]
      const server = twigg({
        "GET /runs/run_9": [Response.json(running), Response.json({ ...running, status: "succeeded" })],
        "GET /chats/chat_1/history": [Response.json({ data: rows, has_more: false })],
        "POST /chats/chat_1/responses": [sse([run("run_10"), ...text("ok"), done()])],
      })
      const { store, chat } = memoryChat({
        chat_id: "chat_1",
        namespace: "ns",
        cursor_message_id: "msg_1",
        orphan: { run_id: "run_9", message_id: "msg_2" },
      })
      yield* collect(
        streamInput({
          chat,
          assistantID: "msg_4",
          history: [
            user("msg_1", [{ type: "text", text: "hi" }]),
            assistant("msg_2", [{ type: "text", text: "the fu" }]),
            user("msg_3", [{ type: "text", text: "next" }]),
          ],
        }),
        server.layer,
      )
      expect(store.repaired).toEqual([{ id: "msg_2", rows: [rows[1]] }])
      expect(server.requests.map((request) => request.key)).toEqual([
        "GET /runs/run_9",
        "GET /runs/run_9",
        "GET /chats/chat_1/history",
        "POST /chats/chat_1/responses",
      ])
      expect(server.requests[3].body).toMatchObject({ input: [{ type: "prompt", text: "next" }] })
      expect(store.state).toEqual({ chat_id: "chat_1", namespace: "ns", cursor_message_id: "msg_3" })
    }),
  )

  it.effect("replays a run that already landed under the same idempotency key", () =>
    Effect.gen(function* () {
      const log: Array<{ name: string; args: unknown }> = []
      const server = twigg({
        "POST /chats/chat_1/responses": [
          Response.json(
            { error: { code: "conflict", message: "idempotency key was already used; it belongs to run 0f1e-22" } },
            { status: 409 },
          ),
        ],
        "GET /runs/0f1e-22": [Response.json({ id: "0f1e-22", status: "succeeded", error: null, cost: "0.5", usage })],
        "GET /chats/chat_1/history": [
          Response.json({
            data: [
              { ordinal: 1, role: "user", part: { type: "prompt", text: "hi" } },
              { ordinal: 2, role: "assistant", part: { type: "reasoning", text: "thinking" } },
              { ordinal: 3, role: "assistant", part: { type: "message", text: "reading" } },
              {
                ordinal: 4,
                role: "assistant",
                part: { type: "tool_call", tool_use_id: "call_1", tool_name: "read", arguments: '{"path":"b.ts"}' },
              },
            ],
            has_more: false,
          }),
        ],
      })
      const { store, chat } = memoryChat({ chat_id: "chat_1", namespace: "ns" })
      const events = yield* collect(
        streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "hi" }])], tools: tools(log) }),
        server.layer,
      )
      expect(events.map((event) => event.type)).toEqual([
        "step-start",
        "reasoning-start",
        "reasoning-delta",
        "reasoning-end",
        "text-start",
        "text-delta",
        "text-end",
        "tool-input-start",
        "tool-call",
        "tool-result",
        "step-finish",
        "finish",
      ])
      expect(log).toEqual([{ name: "read", args: { path: "b.ts" } }])
      expect(events.at(-1)).toMatchObject({ reason: "tool-calls", providerMetadata: { twigg: { cost: "0.5" } } })
      expect(store.state?.cursor_message_id).toBe("msg_1")
    }),
  )

  it.effect("rejects attachments the model can't take before calling Twigg", () =>
    Effect.gen(function* () {
      const server = twigg({})
      const { chat } = memoryChat()
      const error = yield* failure(
        streamInput({
          chat,
          history: [
            user("msg_1", [
              { type: "text", text: "look" },
              { type: "file", mime: "application/pdf", url: "data:application/pdf;base64,AAAA", filename: "a.pdf" },
            ]),
          ],
        }),
        server.layer,
      )
      expect(error.data).toMatchObject({
        isRetryable: false,
        message: "GPT-6-LUNA can't take application/pdf attachments",
      })
      expect(server.requests).toEqual([])
    }),
  )
})

describe("twigg delta", () => {
  const media = { max_attachments_per_part: 20, user: slot(["image/png"]), tool_result: slot([]) }

  test("sends tool results from after the cursor, then a steering prompt", () => {
    const history = [
      user("msg_1", [{ type: "text", text: "old" }]),
      assistant("msg_2", [
        { type: "text", text: "on the server already" },
        completed("call_1", "file body"),
        {
          type: "tool",
          tool: "bash",
          callID: "call_2",
          state: { status: "error", input: {}, error: "denied", time: { start: 0, end: 1 } },
        },
        {
          type: "tool",
          tool: "bash",
          callID: "call_3",
          state: {
            status: "error",
            input: {},
            error: "Tool execution aborted",
            metadata: { interrupted: true, output: "partial log" },
            time: { start: 0, end: 1 },
          },
        },
        { ...completed("call_4", "server side"), metadata: { providerExecuted: true } },
      ]),
      user("msg_3", [
        { type: "text", text: "also do this" },
        { type: "text", text: "hidden", ignored: true },
        { type: "text", text: "<reminder>", synthetic: true },
      ]),
    ]
    expect(TwiggRuntime.delta(history, "msg_1", media)).toEqual({
      input: [
        { type: "tool_result", tool_use_id: "call_1", tool_name: "read", text: "file body", is_error: false },
        { type: "tool_result", tool_use_id: "call_2", tool_name: "bash", text: "denied", is_error: true },
        { type: "tool_result", tool_use_id: "call_3", tool_name: "bash", text: "partial log", is_error: false },
        { type: "prompt", text: "also do this\n\n<reminder>" },
      ],
      last: MessageID.make("msg_3"),
    })
  })

  test("a new chat starts from the prompts after the last assistant reply", () => {
    const history = [
      user("msg_1", [{ type: "text", text: "asked another provider" }]),
      assistant("msg_2", [{ type: "text", text: "answer" }, completed("call_1", "out")]),
      user("msg_3", [{ type: "text", text: "first try" }]),
      assistant("msg_4", []),
      user("msg_5", [{ type: "text", text: "now on twigg" }]),
    ]
    expect(TwiggRuntime.delta(history, undefined, media).input).toEqual([
      { type: "prompt", text: "first try" },
      { type: "prompt", text: "now on twigg" },
    ])
  })

  test("turns attachments into media, or a note when they can't be sent", () => {
    const png = { mime: "image/png", url: "data:image/png;base64,iVBORw0K", filename: "shot.png" }
    const history = [
      user("msg_1", [
        { type: "text", text: "see" },
        { type: "file", ...png },
        { type: "file", mime: "text/plain", url: "file:///a.txt", filename: "a.txt" },
        { type: "file", mime: "image/png", url: "https://example.com/x.png", filename: "x.png" },
      ]),
      assistant("msg_2", [completed("call_1", "screenshot taken", { attachments: [{ type: "file", ...png }] })]),
    ]
    expect(TwiggRuntime.delta(history, "msg_0", media).input).toEqual([
      {
        type: "tool_result",
        tool_use_id: "call_1",
        tool_name: "read",
        text: "screenshot taken\n\n[shot.png not sent: the model can't take it in a tool result]",
        is_error: false,
      },
      {
        type: "prompt",
        text: "see\n\n[Attached image/png: x.png]",
        media: [{ mime: "image/png", data_base64: "iVBORw0K", filename: "shot.png" }],
      },
    ])
  })
})

describe("twigg instructions", () => {
  const project = "twigg-code/test/p/project"
  const sources = { global: ["G"], project: ["P1", "P2"] }

  test("mirror the instruction hierarchy down the namespace", () => {
    const main = TwiggInstructions.levels(project, sources)
    expect(main.map((level) => [level.namespace, level.mode])).toEqual([
      ["twigg-code/test", "append"],
      [project, "append"],
    ])
    expect(main[0].body.endsWith("\n\nG")).toBe(true)
    expect(main[1].body).toBe("P1\n\nP2")

    const child = TwiggInstructions.levels(`${project}/a/explore`, { ...sources, agent: "EXPLORE" })
    expect(child[1].namespace).toBe(project)
    expect(child[2]).toEqual({ namespace: `${project}/a/explore`, mode: "replace", body: "EXPLORE\n\nG\n\nP1\n\nP2" })
    expect(TwiggInstructions.levels(`${project}/a/general`, sources)[2].body).toBe("")
  })

  it.effect("publishes changed levels only, and withdraws emptied ones", () =>
    Effect.gen(function* () {
      const published = new Map<string, string>()
      const turn = (instructions: TwiggInstructions.Sources, active?: Record<string, { mode: string; body: string }>) =>
        Effect.gen(function* () {
          const server = twigg({ "POST /chats/chat_1/responses": [sse([run(), ...text("ok"), done()])] }, active)
          const { chat } = memoryChat({ chat_id: "chat_1", namespace: project })
          yield* collect(
            streamInput({ chat, history: [user("msg_1", [{ type: "text", text: "hi" }])], instructions, published }),
            server.layer,
          )
          return server
        })

      const first = yield* turn(sources)
      expect(first.config).toEqual(["publish append twigg-code/test", `publish append ${project}`])
      // Same content in the same process: nothing to check.
      expect((yield* turn(sources)).config).toEqual([])
      const changed = yield* turn({ ...sources, project: ["P3"] }, { ...first.active })
      expect(changed.config).toEqual([`publish append ${project}`])
      expect(changed.active[project].body).toBe("P3")
      // A fresh process finds the same bodies already active and publishes nothing.
      published.clear()
      expect((yield* turn({ ...sources, project: ["P3"] }, { ...changed.active })).config).toEqual([])
      expect((yield* turn({ ...sources, project: [] }, { ...changed.active })).config).toEqual([`withdraw ${project}`])
    }),
  )

  it.effect("sends the context block with a prompt, and again only when it changes", () =>
    Effect.gen(function* () {
      const server = twigg({
        "POST /chats/chat_1/responses": [
          sse([run(), ...text("a"), done()]),
          sse([run(), ...text("b"), done()]),
          sse([run(), ...text("c"), done()]),
        ],
      })
      const { store, chat } = memoryChat({ chat_id: "chat_1", namespace: project })
      const history = [user("msg_1", [{ type: "text", text: "hi" }])]
      yield* collect(streamInput({ chat, history, context: "<env>cwd: /a</env>" }), server.layer)
      expect(server.requests[0].body).toMatchObject({
        input: [{ type: "prompt", text: "<system-context>\n<env>cwd: /a</env>\n</system-context>\n\nhi" }],
      })
      const hash = store.state?.context_hash
      expect(hash).toBeString()

      const next = [...history, user("msg_2", [{ type: "text", text: "again" }])]
      yield* collect(streamInput({ chat, history: next, context: "<env>cwd: /a</env>" }), server.layer)
      expect(server.requests[1].body).toMatchObject({ input: [{ type: "prompt", text: "again" }] })

      // A turn with only tool results can't carry the block, so the change waits for the next prompt.
      const tools = [...next, assistant("msg_3", [completed("call_1", "out")])]
      yield* collect(streamInput({ chat, history: tools, context: "<env>cwd: /b</env>" }), server.layer)
      expect(server.requests[2].body).toMatchObject({ input: [{ type: "tool_result", text: "out" }] })
      expect(store.state?.context_hash).toBe(hash)
    }),
  )
})

import { describe, expect } from "bun:test"
import { Effect, Layer, Schema, Stream } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { TwiggClient } from "../../src/twigg/client"
import { it } from "../lib/effect"

const settings = { baseURL: "https://twigg.test/api/v1/", apiKey: "tw_test_key" }
const encoder = new TextEncoder()

function http(handler: (request: HttpClientRequest.HttpClientRequest) => Response) {
  return Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, handler(request)))),
  )
}

function sse(chunks: string[]) {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      chunks.forEach((chunk) => controller.enqueue(encoder.encode(chunk)))
      controller.close()
    },
  })
  return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } })
}

function errorResponse(status: number, error: Record<string, unknown>) {
  return Response.json({ error }, { status })
}

function events(chunks: string[]) {
  return TwiggClient.events(Stream.fromIterable(chunks.map((chunk) => encoder.encode(chunk)))).pipe(
    Stream.runCollect,
    Effect.map((items) => Array.from(items)),
  )
}

const done = {
  stop_reason: "tool_use",
  pending_tool_calls: [{ tool_name: "read", tool_use_id: "call_1" }],
  usage: { input_tokens: 10, output_tokens: 5, reasoning_tokens: 2, cache_read_tokens: 0, cache_write_tokens: 0 },
  cost: null,
  cost_currency: "USD",
  model_served: "gpt-6-luna",
  provider_response_id: null,
  compaction: null,
  server_tools: null,
}

const transcript = [
  `event: run\ndata: ${JSON.stringify({ run_id: "r1", chat_id: "c1", closed_tool_calls: [] })}\n\n`,
  `event: block_start\ndata: {"kind":"text"}\n\n`,
  `event: delta\ndata: {"kind":"text","text":"héllo wörld"}\n\n`,
  `event: block_stop\ndata: {}\n\n`,
  `event: done\ndata: ${JSON.stringify(done)}\n\n`,
].join("")

describe("twigg client events", () => {
  it.effect("parses a whole transcript in one chunk", () =>
    Effect.gen(function* () {
      const items = yield* events([transcript])
      expect(items.map((item) => item.event)).toEqual(["run", "block_start", "delta", "block_stop", "done"])
      expect(items[2]).toEqual({ event: "delta", data: { kind: "text", text: "héllo wörld" } })
      expect(items[4]).toMatchObject({ event: "done", data: { stop_reason: "tool_use", cost: null } })
    }),
  )

  it.effect("reassembles frames split at every byte", () =>
    Effect.gen(function* () {
      const bytes = encoder.encode(transcript)
      const items = yield* TwiggClient.events(
        Stream.fromIterable(Array.from(bytes, (byte) => new Uint8Array([byte]))),
      ).pipe(Stream.runCollect)
      expect(Array.from(items)).toEqual(yield* events([transcript]))
    }),
  )

  it.effect("handles frames split across lines and CRLF line endings", () =>
    Effect.gen(function* () {
      const items = yield* events([
        "event: blo",
        'ck_start\r\ndata: {"kind":"tool_call","tool_name":"read",',
        '"tool_use_id":"call_1"}\r\n\r\nevent: delta\r\ndata: {"kind":"tool_input","text":"{\\"pa"}\r\n\r\n',
      ])
      expect(items).toEqual([
        { event: "block_start", data: { kind: "tool_call", tool_name: "read", tool_use_id: "call_1" } },
        { event: "delta", data: { kind: "tool_input", text: '{"pa' } },
      ])
    }),
  )

  it.effect("skips unknown events and comments", () =>
    Effect.gen(function* () {
      const items = yield* events([
        ': keep-alive\n\nevent: future_thing\ndata: {"x":1}\n\n',
        "event: block_stop\ndata: {}\n\n",
      ])
      expect(items).toEqual([{ event: "block_stop", data: {} }])
    }),
  )

  it.effect("fails with StreamError on an error event", () =>
    Effect.gen(function* () {
      const error = yield* events([
        `event: run\ndata: ${JSON.stringify({ run_id: "r1", chat_id: "c1", closed_tool_calls: [] })}\n\n`,
        'event: error\ndata: {"code":"external_service_error","message":"provider failed","hint":null}\n\n',
      ]).pipe(Effect.flip)
      expect(error).toBeInstanceOf(TwiggClient.StreamError)
      expect(error).toMatchObject({ code: "external_service_error", message: "provider failed" })
    }),
  )

  it.effect("fails with TransportError on a malformed event", () =>
    Effect.gen(function* () {
      const error = yield* events(["event: delta\ndata: {not json\n\n"]).pipe(Effect.flip)
      expect(error).toBeInstanceOf(TwiggClient.TransportError)
    }),
  )
})

describe("twigg client http", () => {
  it.effect("sends auth, query and body, and decodes the response", () => {
    const seen: HttpClientRequest.HttpClientRequest[] = []
    return Effect.gen(function* () {
      const result = yield* TwiggClient.request(settings, Schema.Struct({ id: Schema.String }), {
        method: "POST",
        path: "/chats",
        query: { limit: 5, after_id: undefined },
        body: { namespace: "oc/matti/p/abc" },
      })
      expect(result).toEqual({ id: "c1" })
      expect(seen[0].url).toBe("https://twigg.test/api/v1/chats")
      expect(seen[0].headers.authorization).toBe("Bearer tw_test_key")
      expect(seen[0].urlParams.params).toContainEqual(["limit", "5"])
    }).pipe(
      Effect.provide(
        http((request) => {
          seen.push(request)
          return Response.json({ id: "c1" }, { status: 201 })
        }),
      ),
    )
  })

  it.effect("treats 204 as null", () =>
    TwiggClient.request(settings, Schema.Null, { method: "DELETE", path: "/chats/c1" }).pipe(
      Effect.tap((result) => Effect.sync(() => expect(result).toBeNull())),
      Effect.provide(http(() => new Response(null, { status: 204 }))),
    ),
  )

  const cases = [
    { status: 402, code: "payment_required", message: "balance too low", tag: "TwiggPaymentRequiredError" },
    { status: 404, code: "not_found", message: "no such chat", tag: "TwiggNotFoundError" },
    { status: 422, code: "validation_error", message: "bad", tag: "TwiggValidationError" },
    { status: 429, code: "rate_limited", message: "slow down", tag: "TwiggRateLimitedError" },
    { status: 503, code: "unavailable", message: "down", tag: "TwiggServerError" },
    { status: 401, code: "unauthorized", message: "bad key", tag: "TwiggRequestError" },
  ]
  cases.forEach((item) =>
    it.effect(`maps ${item.status} to ${item.tag}`, () =>
      Effect.gen(function* () {
        const error = yield* TwiggClient.request(settings, Schema.Unknown, { method: "GET", path: "/x" }).pipe(
          Effect.flip,
        )
        expect(error).toMatchObject({ _tag: item.tag, status: item.status, code: item.code, message: item.message })
      }).pipe(Effect.provide(http(() => errorResponse(item.status, { code: item.code, message: item.message })))),
    ),
  )

  it.effect("tells the two kinds of 409 apart", () =>
    Effect.gen(function* () {
      const busy = yield* TwiggClient.request(settings, Schema.Unknown, { method: "POST", path: "/busy" }).pipe(
        Effect.flip,
      )
      expect(busy).toMatchObject({ _tag: "TwiggConflictError", reason: "busy" })
      const reused = yield* TwiggClient.request(settings, Schema.Unknown, { method: "POST", path: "/reused" }).pipe(
        Effect.flip,
      )
      expect(reused).toMatchObject({
        _tag: "TwiggConflictError",
        reason: "idempotency",
        runID: "0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
      })
    }).pipe(
      Effect.provide(
        http((request) =>
          errorResponse(409, {
            code: "conflict",
            message: request.url.endsWith("/busy")
              ? "chat 1234 is being written by another request"
              : "idempotency key was already used; it belongs to run 0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
          }),
        ),
      ),
    ),
  )

  it.effect("keeps the status when the error body isn't JSON", () =>
    Effect.gen(function* () {
      const error = yield* TwiggClient.request(settings, Schema.Unknown, { method: "GET", path: "/x" }).pipe(
        Effect.flip,
      )
      expect(error).toMatchObject({ _tag: "TwiggServerError", status: 502, code: "unknown" })
    }).pipe(Effect.provide(http(() => new Response("<html>Bad gateway</html>", { status: 502 })))),
  )

  it.effect("streams events from a chunked response", () =>
    Effect.gen(function* () {
      const items = yield* TwiggClient.stream(settings, {
        method: "POST",
        path: "/chats/c1/responses",
        body: { model: "gpt-6-luna", input: [] },
      }).pipe(Stream.runCollect)
      expect(Array.from(items).map((item) => item.event)).toEqual(["run", "block_start", "delta", "block_stop", "done"])
    }).pipe(Effect.provide(http(() => sse([transcript.slice(0, 50), transcript.slice(50, 51), transcript.slice(51)])))),
  )

  it.effect("fails a stream before any event on an HTTP error", () =>
    Effect.gen(function* () {
      const error = yield* TwiggClient.stream(settings, { method: "POST", path: "/chats/c1/responses" }).pipe(
        Stream.runCollect,
        Effect.flip,
      )
      expect(error).toMatchObject({ _tag: "TwiggConflictError", reason: "busy" })
    }).pipe(
      Effect.provide(
        http(() => errorResponse(409, { code: "conflict", message: "chat c1 is being written by another request" })),
      ),
    ),
  )
})

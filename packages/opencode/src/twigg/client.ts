import { Effect, Schema, Stream } from "effect"
import { Sse } from "effect/unstable/encoding"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"

export const DEFAULT_BASE_URL = "https://api.twigg.ai/api/v1"

export interface Settings {
  readonly baseURL: string
  readonly apiKey: string
}

export interface Request {
  readonly method: "GET" | "POST" | "PATCH" | "DELETE"
  readonly path: string
  readonly query?: Record<string, string | number | undefined>
  readonly body?: unknown
}

const ErrorBody = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
  details: Schema.optional(Schema.Json),
  hint: Schema.optional(Schema.NullOr(Schema.String)),
})

const ApiErrorFields = { status: Schema.Number, ...ErrorBody.fields }

export class PaymentRequiredError extends Schema.TaggedErrorClass<PaymentRequiredError>()(
  "TwiggPaymentRequiredError",
  ApiErrorFields,
) {}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("TwiggNotFoundError", ApiErrorFields) {}

// Twigg uses 409 for two different situations; `reason` tells them apart so callers can wait or reconcile.
export class ConflictError extends Schema.TaggedErrorClass<ConflictError>()("TwiggConflictError", {
  ...ApiErrorFields,
  reason: Schema.Literals(["busy", "idempotency", "unknown"]),
  runID: Schema.optional(Schema.String),
}) {}

export class ValidationError extends Schema.TaggedErrorClass<ValidationError>()(
  "TwiggValidationError",
  ApiErrorFields,
) {}

export class RateLimitedError extends Schema.TaggedErrorClass<RateLimitedError>()(
  "TwiggRateLimitedError",
  ApiErrorFields,
) {}

export class ServerError extends Schema.TaggedErrorClass<ServerError>()("TwiggServerError", ApiErrorFields) {}

// Any other HTTP error status, e.g. 400, 401 or 413.
export class RequestError extends Schema.TaggedErrorClass<RequestError>()("TwiggRequestError", ApiErrorFields) {}

// An `error` event after the stream opened. The run has started, so the request must not be re-posted.
export class StreamError extends Schema.TaggedErrorClass<StreamError>()("TwiggStreamError", ErrorBody.fields) {}

// The request never got a usable answer: network failure, or a body that isn't what the API documents.
export class TransportError extends Schema.TaggedErrorClass<TransportError>()("TwiggTransportError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export type ApiError =
  | PaymentRequiredError
  | NotFoundError
  | ConflictError
  | ValidationError
  | RateLimitedError
  | ServerError
  | RequestError

export type Error = ApiError | StreamError | TransportError

const Usage = Schema.Struct({
  input_tokens: Schema.Number,
  output_tokens: Schema.Number,
  reasoning_tokens: Schema.Number,
  cache_read_tokens: Schema.Number,
  cache_write_tokens: Schema.Number,
})

const PendingCall = Schema.Struct({ tool_name: Schema.String, tool_use_id: Schema.String })

const frame = <const E extends string, S extends Schema.Top>(event: E, data: S) =>
  Schema.Struct({ event: Schema.Literal(event), data: Schema.fromJsonString(data) })

export const Event = Schema.Union([
  frame(
    "run",
    Schema.Struct({ run_id: Schema.String, chat_id: Schema.String, closed_tool_calls: Schema.Array(Schema.String) }),
  ),
  frame("config_warnings", Schema.Struct({ warnings: Schema.Array(Schema.Json) })),
  frame("translation_warnings", Schema.Struct({ warnings: Schema.Array(Schema.Json) })),
  frame("compacting", Schema.Struct({ blocks: Schema.Number, in_progress_elsewhere: Schema.Number })),
  frame(
    "block_start",
    Schema.Union([
      Schema.Struct({ kind: Schema.Literals(["text", "reasoning", "refusal"]) }),
      Schema.Struct({ kind: Schema.Literal("tool_call"), tool_name: Schema.String, tool_use_id: Schema.String }),
    ]),
  ),
  frame(
    "delta",
    Schema.Struct({ kind: Schema.Literals(["text", "reasoning", "tool_input", "refusal"]), text: Schema.String }),
  ),
  frame("block_stop", Schema.Struct({})),
  frame(
    "done",
    Schema.Struct({
      stop_reason: Schema.Literals(["end_turn", "max_tokens", "tool_use", "stop_sequence", "refusal"]),
      pending_tool_calls: Schema.Array(PendingCall),
      usage: Usage,
      // A decimal string, e.g. "0.0012". Null until the run is settled; read GET /runs/{id} for the final figure.
      cost: Schema.NullOr(Schema.String),
      cost_currency: Schema.String,
      model_served: Schema.NullOr(Schema.String),
      provider_response_id: Schema.NullOr(Schema.String),
      compaction: Schema.NullOr(Schema.Json),
      server_tools: Schema.NullOr(Schema.Json),
    }),
  ),
])
export type Event = typeof Event.Type

export const Chat = Schema.Struct({ id: Schema.String, namespace: Schema.NullOr(Schema.String) })

// GET /runs/{id}. The manifest and timing fields are left out; nothing reads them yet.
export const Run = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["running", "succeeded", "failed", "cancelled"]),
  error: Schema.NullOr(Schema.String),
  cost: Schema.NullOr(Schema.String),
  usage: Schema.NullOr(Usage),
})
export type Run = typeof Run.Type

// One ledger row from GET /chats/{id}/history. Tool-call `arguments` is a JSON string.
export const HistoryRow = Schema.Struct({
  ordinal: Schema.Number,
  role: Schema.String,
  part: Schema.Union([
    Schema.Struct({ type: Schema.Literal("prompt"), text: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("tool_result"),
      tool_use_id: Schema.String,
      tool_name: Schema.String,
      text: Schema.String,
      is_error: Schema.Boolean,
    }),
    Schema.Struct({
      type: Schema.Literal("message"),
      text: Schema.String,
      refusal: Schema.optional(Schema.NullOr(Schema.String)),
    }),
    Schema.Struct({ type: Schema.Literal("reasoning"), text: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("tool_call"),
      tool_use_id: Schema.String,
      tool_name: Schema.String,
      arguments: Schema.String,
    }),
  ]),
})
export type HistoryRow = typeof HistoryRow.Type

export const HistoryPage = Schema.Struct({ data: Schema.Array(HistoryRow), has_more: Schema.Boolean })

const EVENTS = new Set<string>(Event.members.map((member) => member.fields.event.literal))
const decodeEvent = Schema.decodeUnknownEffect(Event)
const decodeError = Schema.decodeUnknownEffect(Schema.fromJsonString(ErrorBody))

export const request = Effect.fnUntraced(function* <A>(settings: Settings, schema: Schema.Decoder<A>, input: Request) {
  const response = yield* send(settings, input, "application/json")
  const body =
    response.status === 204
      ? null
      : yield* response.json.pipe(
          Effect.mapError((cause) => new TransportError({ message: `Unreadable response from ${input.path}`, cause })),
        )
  return yield* Schema.decodeUnknownEffect(schema)(body).pipe(
    Effect.mapError((cause) => new TransportError({ message: `Unexpected response from ${input.path}`, cause })),
  )
})

// HTTP errors fail before the first event. After the stream opens, an `error` event fails it with StreamError.
export const stream = (settings: Settings, input: Request) =>
  send(settings, input, "text/event-stream").pipe(
    Effect.map((response) =>
      events(
        response.stream.pipe(
          Stream.mapError((cause) => new TransportError({ message: `Stream from ${input.path} broke`, cause })),
        ),
      ),
    ),
    Stream.unwrap,
  )

// Frames can be split across chunks, and one chunk can carry several frames. Unknown event names are skipped so new
// server events don't break older clients.
export const events = <E>(bytes: Stream.Stream<Uint8Array, E>) =>
  bytes.pipe(
    Stream.decodeText(),
    Stream.pipeThroughChannel(Sse.decode()),
    Stream.catchTag("Retry", () => Stream.empty),
    Stream.filter((item) => item.event === "error" || EVENTS.has(item.event)),
    Stream.mapEffect((item) => {
      if (item.event === "error")
        return decodeError(item.data).pipe(
          Effect.mapError((cause) => new TransportError({ message: "Malformed error event", cause })),
          Effect.flatMap((body) => Effect.fail(new StreamError(body))),
        )
      return decodeEvent(item).pipe(
        Effect.mapError((cause) => new TransportError({ message: `Malformed ${item.event} event`, cause })),
      )
    }),
  )

const send = Effect.fnUntraced(function* (settings: Settings, input: Request, accept: string) {
  const http = yield* HttpClient.HttpClient
  const request = HttpClientRequest.make(input.method)(`${settings.baseURL.replace(/\/+$/, "")}${input.path}`, {
    urlParams: input.query,
    accept,
  }).pipe(HttpClientRequest.bearerToken(settings.apiKey))
  const body =
    input.body === undefined
      ? request
      : yield* HttpClientRequest.bodyJson(request, input.body).pipe(
          Effect.mapError((cause) => new TransportError({ message: `Could not encode body for ${input.path}`, cause })),
        )
  const response = yield* http
    .execute(body)
    .pipe(Effect.mapError((cause) => new TransportError({ message: `Request to ${input.path} failed`, cause })))
  if (response.status < 400) return response
  return yield* apiError(response)
})

const apiError = Effect.fnUntraced(function* (response: HttpClientResponse.HttpClientResponse) {
  const body = yield* HttpClientResponse.schemaBodyJson(Schema.Struct({ error: ErrorBody }))(response).pipe(
    Effect.map((envelope) => envelope.error),
    Effect.catch(() => Effect.succeed({ code: "unknown", message: `HTTP ${response.status}` })),
  )
  const fields = { status: response.status, ...body }
  if (response.status === 402) return yield* new PaymentRequiredError(fields)
  if (response.status === 404) return yield* new NotFoundError(fields)
  if (response.status === 409) return yield* new ConflictError({ ...fields, ...conflict(body.message) })
  if (response.status === 422) return yield* new ValidationError(fields)
  if (response.status === 429) return yield* new RateLimitedError(fields)
  if (response.status >= 500) return yield* new ServerError(fields)
  return yield* new RequestError(fields)
})

// Messages observed on 2026-09-23: "chat … is being written by another request" and
// "idempotency key was already used; it belongs to run <id>".
function conflict(message: string) {
  const run = message.match(/belongs to run ([0-9a-f-]+)/i)
  if (run) return { reason: "idempotency" as const, runID: run[1] }
  if (message.includes("idempotency")) return { reason: "idempotency" as const }
  if (message.includes("being written")) return { reason: "busy" as const }
  return { reason: "unknown" as const }
}

export * as TwiggClient from "./client"

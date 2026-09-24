import { FSUtil } from "@opencode-ai/core/fs-util"
import { Hash } from "@opencode-ai/core/util/hash"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { LLMEvent, Usage, type FinishReason } from "@opencode-ai/llm"
import { asSchema, type ModelMessage, type Tool } from "ai"
import { Effect, Option, Schedule, Schema, Stream } from "effect"
import type { HttpClient } from "effect/unstable/http"
import { InstanceState } from "@/effect/instance-state"
import type { Provider } from "@/provider/provider"
import { PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { errorMessage } from "@/util/error"
import { isRecord } from "@/util/record"
import { TwiggClient } from "./client"
import { TwiggInstructions } from "./instructions"
import { TwiggModels } from "./models"
import { TwiggNamespace } from "./namespace"
import { TwiggPending } from "./pending"

// Kept in session.metadata.twigg. Twigg holds the model context; this only links the local session to its chat.
export const State = Schema.Struct({
  chat_id: Schema.String,
  namespace: Schema.String,
  // The newest local message whose content the chat already holds. Everything after it goes in the next request.
  cursor_message_id: Schema.optional(Schema.String),
  // A run whose stream closed before `done` (abort or a dropped connection). It keeps running on the server, so the
  // next turn waits for it and copies what it produced into the local message.
  orphan: Schema.optional(Schema.Struct({ run_id: Schema.String, message_id: Schema.String })),
  // The latest run that failed on the server (an `error` event). Its input is in the ledger already, so a retry with
  // nothing new to send asks Twigg to run it again (`retry_of`) instead of posting the input twice.
  failed_run: Schema.optional(Schema.String),
  // Hash of the last context block the chat received, so it is only sent again when it changes.
  context_hash: Schema.optional(Schema.String),
  // Set on a session created from a chat found on Twigg; its history is imported when it is first opened.
  stub: Schema.optional(Schema.Boolean),
  // The newest history ordinal that TwiggSync has seen, for catching up with parts written elsewhere.
  synced_ordinal: Schema.optional(Schema.Number),
})
export type State = typeof State.Type

export const decodeState = Schema.decodeUnknownOption(State)

export interface Chat {
  readonly load: Effect.Effect<State | undefined>
  readonly save: (state: State) => Effect.Effect<void>
  // Where the chat is created on the first turn.
  readonly open: Effect.Effect<{ readonly namespace: string; readonly user_metadata: Record<string, unknown> }>
  // Replaces an interrupted local message's content with what its orphaned run actually produced.
  readonly repair: (messageID: string, rows: readonly TwiggClient.HistoryRow[]) => Effect.Effect<void>
  // Sent as user_metadata, so every part this session writes can be told apart from parts written elsewhere.
  readonly tag: Record<string, string>
}

export interface Input {
  readonly settings: TwiggClient.Settings
  readonly model: Provider.Model
  // The local assistant message this request fills. It is the idempotency key, so a retried request never runs twice.
  readonly assistantID: string
  // Local history, oldest first, without the assistant message being filled.
  readonly history: readonly SessionV1.WithParts[]
  readonly tools: Record<string, Tool>
  readonly messages: ModelMessage[]
  readonly maxTokens?: number
  readonly reasoningEffort?: string
  readonly abort: AbortSignal
  readonly chat: Chat
  // Published to the chat's namespace chain before the request (see TwiggInstructions.levels).
  readonly instructions: TwiggInstructions.Sources
  // Per-machine and per-turn system text (env, date, model, skills, MCP). It can't live in a shared namespace, so it
  // is prepended to the prompt whenever it changes.
  readonly context: string
  // Which instruction levels this process already checked, shared across requests.
  readonly published: Map<string, string>
}

type Media = { mime: string; data_base64: string; filename?: string }

export type Part =
  | { type: "prompt"; text?: string; media?: Media[] }
  | { type: "tool_result"; tool_use_id: string; tool_name: string; text: string; is_error: boolean; media?: Media[] }

const MediaLimits = TwiggModels.Model.fields.media
type MediaSlot = typeof MediaLimits.Type.user

const REASONS: Record<string, FinishReason> = {
  end_turn: "stop",
  stop_sequence: "stop",
  tool_use: "tool-calls",
  max_tokens: "length",
  refusal: "content-filter",
}

const decodeArguments = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)
const decodeMedia = Schema.decodeUnknownOption(MediaLimits)

// One Twigg request per loop iteration: send what's new since the cursor, stream the reply, run the tools the model
// asked for, and finish. The session loop then comes round again and the next delta is just those tool results.
export function stream(input: Input): Stream.Stream<LLMEvent, unknown, HttpClient.HttpClient> {
  return Stream.unwrap(
    Effect.gen(function* () {
      const loaded = yield* input.chat.load
      const settled = loaded?.orphan ? yield* settle(input, loaded, loaded.orphan) : loaded
      const media = Option.getOrUndefined(decodeMedia(input.model.options.twigg?.media))
      const next = delta(input.history, settled?.cursor_message_id, media)
      const retry = next.input.length === 0 ? settled?.failed_run : undefined
      const problem = retry ? undefined : checkRequest(input, next.input, media)
      if (problem) return yield* Effect.fail(apiError(problem, false))
      const state = settled ?? (yield* create(input, next.input))
      yield* TwiggInstructions.ensure(
        input.settings,
        TwiggInstructions.levels(state.namespace, input.instructions),
        input.published,
      )
      const context = withContext(next.input, input.context, state.context_hash)
      const saved = { current: state }
      const save = (patch: Partial<State>) =>
        Effect.suspend(() => {
          saved.current = { ...saved.current, ...patch }
          return input.chat.save(saved.current)
        })
      const events = TwiggClient.stream(input.settings, {
        method: "POST",
        path: `/chats/${state.chat_id}/responses`,
        body: {
          model: input.model.api.id,
          input: context.parts,
          // A retry needs its own key; the failed run holds the original one.
          idempotency_key: retry ? `${input.assistantID}-retry-${retry}` : input.assistantID,
          retry_of: retry,
          max_tokens: input.maxTokens,
          reasoning_effort: input.reasoningEffort,
          tools: definitions(input.tools),
          user_metadata: input.chat.tag,
        },
      })
      return translate(events, {
        // The server accepted the input, so the cursor moves. Until `done`, the run counts as an orphan.
        run: (runID) =>
          save({
            cursor_message_id: next.last,
            orphan: { run_id: runID, message_id: input.assistantID },
            failed_run: undefined,
            ...(context.hash ? { context_hash: context.hash } : {}),
          }),
        done: save({ orphan: undefined }),
        failed: (runID) => save({ orphan: undefined, failed_run: runID }),
        execute: (calls) => execute(input, calls),
      }).pipe(
        // The input already landed under this idempotency key, e.g. a retry after the connection broke before `run`.
        Stream.catchIf(landed, (error) =>
          reconcile(
            input,
            state.chat_id,
            error.runID,
            save({
              cursor_message_id: next.last,
              orphan: undefined,
              ...(context.hash ? { context_hash: context.hash } : {}),
            }),
          ),
        ),
      )
    }),
  ).pipe(Stream.mapError((error) => (isTwiggError(error) ? toAPIError(error) : error)))
}

// Side calls (titles, project-copy names) have no chat. They need throwaway calls, which Twigg doesn't have yet.
export function respond(input: {
  readonly settings: TwiggClient.Settings
  readonly model: Provider.Model
  readonly system: string[]
  readonly messages: ModelMessage[]
  readonly maxTokens?: number
}) {
  const text = input.messages
    .flatMap((message) => {
      if (message.role !== "user") return []
      if (typeof message.content === "string") return [message.content]
      return message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
    })
    .join("\n\n")
  // TODO(twigg-api#5): the stub fails with TwiggApiPending; callers (e.g. title generation) keep their fallback.
  return translate(
    TwiggPending.respondOnce(input.settings, {
      model: input.model.api.id,
      input: [{ type: "prompt", text }],
      max_tokens: input.maxTokens,
      instructions: input.system.join("\n"),
    }),
    { run: () => Effect.void, done: Effect.void, failed: () => Effect.void, execute: () => Effect.succeed([]) },
  ).pipe(Stream.mapError((error) => (isTwiggError(error) ? toAPIError(error) : error)))
}

// The parts to send: tool results from assistant messages after the cursor, then the user prompts after it. A steering
// prompt that arrived mid-run lands after the assistant message, so it goes in the same input as the tool results.
export function delta(
  history: readonly SessionV1.WithParts[],
  cursor: string | undefined,
  media: typeof MediaLimits.Type | undefined,
) {
  // Without a cursor the chat is new, but the session may not be: a fork, or a session that used another provider
  // before. A new chat has no open tool calls, and replaying the old transcript as prompts would garble it, so it
  // starts from the prompts after the last reply the model wrote.
  const start =
    cursor === undefined
      ? history.findLastIndex(
          (message) => message.info.role === "assistant" && message.parts.some((part) => !isUserExecuted(part)),
        ) + 1
      : 0
  const fresh = history.slice(start).filter((message) => cursor === undefined || message.info.id > cursor)
  const tools = fresh
    .filter((message) => message.info.role === "assistant")
    .flatMap((message) => message.parts.filter((part): part is SessionV1.ToolPart => part.type === "tool"))
    .filter((part) => !part.metadata?.providerExecuted)
  const input: Part[] = [
    ...tools.filter((part) => !isUserExecuted(part)).map((part) => toolResult(part, media?.tool_result)),
    // Prompts in order. A tool the user ran (a subtask or shell command) was never called by the model, so Twigg
    // gets it as text after the prompt that started it.
    ...fresh.flatMap((message): Part[] =>
      message.info.role === "user"
        ? prompt(message.parts)
        : message.parts.filter(isUserExecuted).map((part) => ({ type: "prompt" as const, text: userToolText(part) })),
    ),
  ]
  return { input, last: history.at(-1)?.info.id }
}

// Builds the Chat store on top of the local session's metadata.
export const sessionChat = Effect.fn("TwiggRuntime.sessionChat")(function* (input: {
  sessionID: SessionID
  parentSessionID?: SessionID
  agent: string
  device: string
}) {
  const sessions = yield* Session.Service
  const fs = yield* FSUtil.Service
  const ctx = yield* InstanceState.context
  const install = yield* TwiggNamespace.installID().pipe(Effect.provideService(FSUtil.Service, fs))
  const load = (sessionID: SessionID) =>
    sessions.get(sessionID).pipe(
      Effect.orDie,
      Effect.map((session) => Option.getOrUndefined(decodeState(session.metadata?.twigg))),
    )
  return {
    load: load(input.sessionID),
    save: (state) =>
      sessions.get(input.sessionID).pipe(
        Effect.orDie,
        Effect.flatMap((session) =>
          sessions.setMetadata({ sessionID: input.sessionID, metadata: { ...session.metadata, twigg: state } }),
        ),
      ),
    open: Effect.gen(function* () {
      const project = TwiggNamespace.namespaceFor({
        projectID: ctx.project.id,
        directory: ctx.directory,
        profile: install,
        device: input.device,
      })
      const parent = input.parentSessionID ? yield* load(input.parentSessionID) : undefined
      // Subagent chats sit under their agent, so the agent's published prompt applies to them (Phase 2b).
      const namespace = input.parentSessionID ? `${project}/a/${TwiggNamespace.segment(input.agent)}` : project
      const invalid = TwiggNamespace.validate(namespace)
      if (invalid) return yield* Effect.die(new Error(`Invalid Twigg namespace ${namespace}: ${invalid}`))
      return {
        namespace,
        user_metadata: {
          device: input.device,
          directory: ctx.directory,
          session_id: input.sessionID,
          ...(parent ? { parent_chat_id: parent.chat_id } : {}),
        },
      }
    }),
    repair: (messageID, rows) => repair(sessions, input.sessionID, messageID, rows),
    tag: tag(install, input.sessionID),
  } satisfies Chat
})

type Call = { readonly id: string; readonly name: string; readonly input: unknown; readonly invalid?: string }

type Hooks = {
  readonly run: (runID: string) => Effect.Effect<void>
  readonly done: Effect.Effect<void>
  // The run failed on the server (an `error` event), so it is over and no longer an orphan.
  readonly failed: (runID: string) => Effect.Effect<void>
  readonly execute: (calls: readonly Call[]) => Effect.Effect<LLMEvent[]>
}

// Maps Twigg SSE events to LLMEvents. Tools run once `done` names the calls that are really pending, so arguments from
// a stream that failed halfway are never executed.
function translate<R>(
  events: Stream.Stream<TwiggClient.Event, TwiggClient.Error | TwiggPending.TwiggApiPending, R>,
  hooks: Hooks,
): Stream.Stream<LLMEvent, unknown, R> {
  const ctx = {
    runID: "",
    finished: false,
    blocks: 0,
    block: undefined as { kind: "text" | "reasoning" | "tool"; id: string; name: string; args: string } | undefined,
    calls: new Map<string, Call>(),
  }
  return events.pipe(
    Stream.mapEffect(
      Effect.fnUntraced(function* (event: TwiggClient.Event) {
        switch (event.event) {
          case "run":
            ctx.runID = event.data.run_id
            yield* hooks.run(event.data.run_id)
            if (event.data.closed_tool_calls.length > 0)
              yield* Effect.logInfo("twigg closed unanswered tool calls", { calls: event.data.closed_tool_calls })
            return [LLMEvent.stepStart({ index: 0 })]
          case "config_warnings":
          case "translation_warnings": {
            // Subagent levels replace on purpose, and Twigg reports that on every turn even when no org-wide
            // instruction exists (seen 2026-09-24).
            const warnings = event.data.warnings.filter(
              (warning) => !(isRecord(warning) && warning.code === "global_instruction_replaced"),
            )
            if (warnings.length > 0)
              yield* Effect.logWarning(`twigg ${event.event}`, { warnings: JSON.stringify(warnings) })
            return []
          }
          case "compacting":
            yield* Effect.logInfo("twigg is compacting the chat history", event.data)
            return []
          case "block_start": {
            const id = event.data.kind === "tool_call" ? event.data.tool_use_id : `${ctx.runID}-${ctx.blocks++}`
            if (event.data.kind === "tool_call") {
              ctx.block = { kind: "tool", id, name: event.data.tool_name, args: "" }
              return [LLMEvent.toolInputStart({ id, name: event.data.tool_name })]
            }
            // A refusal is shown as text; `done` then reports stop_reason "refusal".
            const kind = event.data.kind === "reasoning" ? "reasoning" : "text"
            ctx.block = { kind, id, name: "", args: "" }
            return [kind === "reasoning" ? LLMEvent.reasoningStart({ id }) : LLMEvent.textStart({ id })]
          }
          case "delta": {
            const block = ctx.block
            if (!block) return []
            if (block.kind === "tool") {
              block.args += event.data.text
              return [LLMEvent.toolInputDelta({ id: block.id, name: block.name, text: event.data.text })]
            }
            if (block.kind === "reasoning") return [LLMEvent.reasoningDelta({ id: block.id, text: event.data.text })]
            return [LLMEvent.textDelta({ id: block.id, text: event.data.text })]
          }
          case "block_stop": {
            const block = ctx.block
            ctx.block = undefined
            if (!block) return []
            if (block.kind === "reasoning") return [LLMEvent.reasoningEnd({ id: block.id })]
            if (block.kind === "text") return [LLMEvent.textEnd({ id: block.id })]
            const call = parseCall(block.id, block.name, block.args)
            ctx.calls.set(call.id, call)
            return [
              LLMEvent.toolInputEnd({ id: call.id, name: call.name }),
              LLMEvent.toolCall({ id: call.id, name: call.name, input: call.input }),
            ]
          }
          case "done": {
            ctx.finished = true
            yield* hooks.done
            const results = yield* hooks.execute(
              event.data.pending_tool_calls.map(
                (call) =>
                  ctx.calls.get(call.tool_use_id) ?? {
                    id: call.tool_use_id,
                    name: call.tool_name,
                    input: {},
                    invalid: "The tool call arrived without arguments",
                  },
              ),
            )
            const reason =
              event.data.pending_tool_calls.length > 0 ? "tool-calls" : (REASONS[event.data.stop_reason] ?? "unknown")
            return [...results, ...finish(reason, event.data.usage, event.data.cost, ctx.runID)]
          }
        }
      }),
    ),
    Stream.flatMap((items) => Stream.fromIterable(items)),
    Stream.concat(
      Stream.suspend(() =>
        ctx.finished
          ? Stream.empty
          : Stream.fail(
              new TwiggClient.TransportError({ message: "The Twigg stream ended before the response finished" }),
            ),
      ),
    ),
    Stream.catchTag("TwiggStreamError", (error) =>
      Stream.fromEffect(hooks.failed(ctx.runID).pipe(Effect.andThen(Effect.fail(error)))),
    ),
    // After `run`, the server owns the turn: a retry would post the same input again, so nothing past here retries.
    // Earlier errors stay raw so the caller can still handle a 409 before the stream starts.
    Stream.mapError((error) => (isTwiggError(error) && ctx.runID !== "" ? toAPIError(error, true) : error)),
  )
}

// Replays a run that already happened (found through its idempotency key) as if it had just streamed.
function reconcile(input: Input, chatID: string, runID: string, landed: Effect.Effect<void>) {
  return Stream.unwrap(
    Effect.gen(function* () {
      const run = yield* finished(input.settings, runID)
      yield* landed
      if (run.status === "failed") return Stream.fail(apiError(run.error ?? "The Twigg response failed", false))
      const rows = yield* tail(input.settings, chatID)
      const calls = rows.flatMap((row) =>
        row.part.type === "tool_call" ? [parseCall(row.part.tool_use_id, row.part.tool_name, row.part.arguments)] : [],
      )
      const replay = rows.flatMap((row, index): LLMEvent[] => {
        const id = `${runID}-${index}`
        if (row.part.type === "message") {
          const text = row.part.text || row.part.refusal || ""
          return [LLMEvent.textStart({ id }), LLMEvent.textDelta({ id, text }), LLMEvent.textEnd({ id })]
        }
        if (row.part.type === "reasoning")
          return [
            LLMEvent.reasoningStart({ id }),
            LLMEvent.reasoningDelta({ id, text: row.part.text }),
            LLMEvent.reasoningEnd({ id }),
          ]
        if (row.part.type !== "tool_call") return []
        const call = calls.find((item) => item.id === (row.part.type === "tool_call" ? row.part.tool_use_id : ""))
        if (!call) return []
        return [
          LLMEvent.toolInputStart({ id: call.id, name: call.name }),
          LLMEvent.toolCall({ id: call.id, name: call.name, input: call.input }),
        ]
      })
      const results = yield* execute(input, calls)
      const usage = run.usage ?? {
        input_tokens: 0,
        output_tokens: 0,
        reasoning_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
      }
      return Stream.fromIterable([
        LLMEvent.stepStart({ index: 0 }),
        ...replay,
        ...results,
        ...finish(calls.length > 0 ? "tool-calls" : "stop", usage, run.cost, runID),
      ])
    }),
  )
}

// Waits for an orphaned run to finish, then brings the local message in line with what the model really produced.
const settle = Effect.fnUntraced(function* (input: Input, state: State, orphan: NonNullable<State["orphan"]>) {
  yield* Effect.logInfo("waiting for the previous twigg response to finish", { runID: orphan.run_id })
  // TODO(twigg-api#1): once runs can be cancelled, an aborted run ends straight away and this wait is short.
  yield* finished(input.settings, orphan.run_id).pipe(
    Effect.andThen(tail(input.settings, state.chat_id)),
    Effect.flatMap((rows) => input.chat.repair(orphan.message_id, rows)),
    Effect.catchTag("TwiggNotFoundError", () => Effect.void),
  )
  const next = { ...state, orphan: undefined }
  yield* input.chat.save(next)
  return next
})

const finished = (settings: TwiggClient.Settings, runID: string) =>
  TwiggClient.request(settings, TwiggClient.Run, { method: "GET", path: `/runs/${runID}` }).pipe(
    Effect.repeat({ until: (run) => run.status !== "running", schedule: Schedule.spaced("1 second") }),
    Effect.timeoutOrElse({
      duration: "10 minutes",
      orElse: () => Effect.fail(apiError("The previous Twigg response is still running. Try again shortly.", false)),
    }),
  )

// The newest run's output: the assistant rows after the last user-side row (prompt or tool result).
const tail = (settings: TwiggClient.Settings, chatID: string) =>
  TwiggClient.request(settings, TwiggClient.HistoryPage, {
    method: "GET",
    path: `/chats/${chatID}/history`,
    query: { limit: 100 },
  }).pipe(Effect.map((page) => page.data.slice(page.data.findLastIndex((row) => row.role === "user") + 1)))

const create = Effect.fnUntraced(function* (input: Input, parts: readonly Part[]) {
  const opened = yield* input.chat.open
  const first = parts
    .find((part) => part.type === "prompt")
    ?.text?.replace(/\s+/g, " ")
    .trim()
  const chat = yield* TwiggClient.request(input.settings, TwiggClient.Chat, {
    method: "POST",
    path: "/chats",
    body: {
      namespace: opened.namespace,
      title: first ? first.slice(0, 80) : undefined,
      user_metadata: opened.user_metadata,
    },
  })
  const state: State = { chat_id: chat.id, namespace: opened.namespace }
  yield* input.chat.save(state)
  return state
})

function execute(input: Input, calls: readonly Call[]) {
  return Effect.forEach(
    calls,
    (call): Effect.Effect<LLMEvent> => {
      const tool = input.tools[call.name]
      if (call.invalid)
        return Effect.succeed(LLMEvent.toolError({ id: call.id, name: call.name, message: call.invalid }))
      if (!tool?.execute)
        return Effect.succeed(
          LLMEvent.toolError({ id: call.id, name: call.name, message: `Unknown tool: ${call.name}` }),
        )
      const run = tool.execute
      return Effect.tryPromise({
        try: async () =>
          run(call.input, { toolCallId: call.id, messages: input.messages, abortSignal: input.abort }) as unknown,
        catch: (error) => error,
      }).pipe(
        Effect.map((value) => LLMEvent.toolResult({ id: call.id, name: call.name, result: { type: "json", value } })),
        Effect.catch((error) =>
          Effect.succeed(LLMEvent.toolError({ id: call.id, name: call.name, message: errorMessage(error), error })),
        ),
      )
    },
    { concurrency: "unbounded" },
  )
}

function finish(
  reason: FinishReason,
  usage: NonNullable<TwiggClient.Run["usage"]>,
  cost: string | null,
  runID: string,
): LLMEvent[] {
  const tokens = new Usage({
    // Twigg's input_tokens excludes cache reads and writes; opencode's inputTokens includes them.
    inputTokens: usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens,
    nonCachedInputTokens: usage.input_tokens,
    cacheReadInputTokens: usage.cache_read_tokens,
    cacheWriteInputTokens: usage.cache_write_tokens,
    // Already includes reasoning.
    outputTokens: usage.output_tokens,
    reasoningTokens: usage.reasoning_tokens,
    totalTokens: usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens + usage.output_tokens,
  })
  // Session.getUsage prefers this cost. Null means not settled yet, and opencode falls back to the model's rates.
  const providerMetadata = { twigg: { cost, run_id: runID } }
  return [
    LLMEvent.stepFinish({ index: 0, reason, usage: tokens, providerMetadata }),
    LLMEvent.finish({ reason, usage: tokens, providerMetadata }),
  ]
}

function parseCall(id: string, name: string, args: string): Call {
  const parsed = decodeArguments(args.trim() === "" ? "{}" : args)
  if (Option.isSome(parsed)) return { id, name, input: parsed.value }
  return { id, name, input: {}, invalid: `The tool arguments were not valid JSON: ${args}` }
}

function definitions(tools: Record<string, Tool>) {
  const items = Object.entries(tools)
    // Only the AI SDK path repairs calls through `invalid`; Twigg calls never reach it.
    .filter(([name]) => name !== "invalid")
    .map(([name, tool]) => ({
      name,
      description: tool.description ?? "",
      input_schema: asSchema(tool.inputSchema).jsonSchema,
    }))
  return items.length > 0 ? items : undefined
}

function isUserExecuted(part: SessionV1.Part): part is SessionV1.ToolPart {
  return part.type === "tool" && part.metadata?.userExecuted === true
}

function userToolText(part: SessionV1.ToolPart) {
  const result =
    part.state.status === "completed"
      ? part.state.output
      : part.state.status === "error"
        ? `Error: ${part.state.error}`
        : "(still running)"
  return `The user ran the ${part.tool} tool with ${JSON.stringify(part.state.input)}. Result:\n${result}`
}

// Adds the context block to the first prompt when it changed since the chat last saw it. A request with only tool
// results carries no prompt, so the block waits for the next one.
function withContext(parts: readonly Part[], context: string, sent: string | undefined) {
  const hash = context.trim() === "" ? undefined : Hash.fast(context)
  const index = parts.findIndex((part) => part.type === "prompt")
  if (!hash || hash === sent || index === -1) return { parts, hash: undefined }
  const block = `<system-context>\n${context}\n</system-context>`
  return {
    parts: parts.map((part, i) =>
      i === index && part.type === "prompt" ? { ...part, text: part.text ? `${block}\n\n${part.text}` : block } : part,
    ),
    hash,
  }
}

function prompt(parts: readonly SessionV1.Part[]): Part[] {
  const files = parts
    .filter((part): part is SessionV1.FilePart => part.type === "file")
    // text/plain files and directories are already inlined as text parts when the message is created.
    .filter((part) => part.mime !== "text/plain" && part.mime !== "application/x-directory")
  // Twigg media is for images, PDFs and the like. Other text files go in as text.
  const inline = files.filter((file) => isText(file.mime))
  const binary = files.filter((file) => !isText(file.mime))
  const media = binary.flatMap((file) => Option.toArray(toMedia(file)))
  const text = [
    ...parts.flatMap((part) => {
      if (part.type === "text" && !part.ignored && part.text !== "") return [part.text]
      if (part.type === "subtask") return ["The following tool was executed by the user"]
      return []
    }),
    ...inline.map((file) =>
      Option.match(toMedia(file), {
        onNone: () => `[Attached ${file.mime}: ${file.filename ?? file.url}]`,
        onSome: (item) =>
          `<file name="${file.filename ?? "attachment"}" type="${file.mime}">\n${Buffer.from(item.data_base64, "base64").toString("utf8")}\n</file>`,
      }),
    ),
    ...binary
      .filter((file) => Option.isNone(toMedia(file)))
      .map((file) => `[Attached ${file.mime}: ${file.filename ?? file.url}]`),
  ].join("\n\n")
  if (!text && media.length === 0) return []
  return [{ type: "prompt", ...(text ? { text } : {}), ...(media.length > 0 ? { media } : {}) }]
}

function isText(mime: string) {
  return mime.startsWith("text/") || /^application\/(json|xml|x-yaml|yaml|javascript|typescript)(;|$)/.test(mime)
}

function toolResult(part: SessionV1.ToolPart, slot: MediaSlot | undefined): Part {
  const base = { type: "tool_result" as const, tool_use_id: part.callID, tool_name: part.tool }
  if (part.state.status === "completed") {
    const attachments = (part.state.attachments ?? []).flatMap((file) =>
      Option.toArray(toMedia(file)).map((media) => ({ file, media })),
    )
    const sent = attachments.filter((item) => slot && fits(slot, item.media))
    const skipped = attachments.filter((item) => !slot || !fits(slot, item.media))
    return {
      ...base,
      text: [
        part.state.output,
        ...skipped.map(
          (item) => `[${item.file.filename ?? item.file.mime} not sent: the model can't take it in a tool result]`,
        ),
      ].join("\n\n"),
      is_error: false,
      ...(sent.length > 0 ? { media: sent.map((item) => item.media) } : {}),
    }
  }
  if (part.state.status === "error") {
    // An interrupted tool can still have produced output worth keeping, e.g. a shell command's partial log.
    const output = part.state.metadata?.interrupted === true ? part.state.metadata.output : undefined
    if (typeof output === "string") return { ...base, text: output, is_error: false }
    return { ...base, text: part.state.error, is_error: true }
  }
  return { ...base, text: "[Tool execution was interrupted]", is_error: true }
}

// Fails early with a readable reason instead of letting Twigg answer 422 for an attachment the model can't take.
function checkRequest(input: Input, parts: readonly Part[], media: typeof MediaLimits.Type | undefined) {
  if (parts.length === 0) return "There is nothing new to send to Twigg"
  const limit = input.model.options.twigg?.max_tool_definitions
  const tools = Object.keys(input.tools).filter((name) => name !== "invalid").length
  if (typeof limit === "number" && tools > limit)
    return `${input.model.name} accepts at most ${limit} tools, but ${tools} are enabled. Disable some tools or MCP servers.`
  const attachments = parts.flatMap((part) => (part.type === "prompt" ? (part.media ?? []) : []))
  if (attachments.length === 0) return
  const slot = media?.user
  const count = slot?.max_attachments_per_request
  if (typeof count === "number" && attachments.length > count)
    return `${input.model.name} accepts at most ${count} attachments per message`
  const rejected = attachments.find((item) => !slot || !fits(slot, item))
  if (!rejected) return
  if (!slot?.mime_types.includes(rejected.mime)) return `${input.model.name} can't take ${rejected.mime} attachments`
  return `${rejected.filename ?? rejected.mime} is larger than the ${slot.max_bytes_per_attachment} bytes ${input.model.name} accepts`
}

function fits(slot: MediaSlot, media: Media) {
  if (!slot.mime_types.includes(media.mime)) return false
  return (
    slot.max_bytes_per_attachment === null ||
    Buffer.byteLength(media.data_base64, "base64") <= slot.max_bytes_per_attachment
  )
}

// Attachments are stored as data URLs. Twigg wants the bare base64.
function toMedia(file: { mime: string; url: string; filename?: string }): Option.Option<Media> {
  const match = file.url.match(/^data:[^,]*;base64,(.*)$/s)
  if (!match) return Option.none()
  return Option.some({ mime: file.mime, data_base64: match[1], ...(file.filename ? { filename: file.filename } : {}) })
}

const repair = Effect.fnUntraced(function* (
  sessions: Session.Interface,
  sessionID: SessionID,
  messageID: string,
  rows: readonly TwiggClient.HistoryRow[],
) {
  if (rows.length === 0) return
  const message = (yield* sessions.messages({ sessionID }).pipe(Effect.orDie)).find(
    (item) => item.info.id === messageID,
  )
  if (!message) return
  yield* Effect.forEach(
    message.parts.filter((part) => part.type === "text" || part.type === "reasoning"),
    (part) => sessions.removePart({ sessionID, messageID: message.info.id, partID: part.id }),
  )
  const known = new Set(message.parts.flatMap((part) => (part.type === "tool" ? [part.callID] : [])))
  const now = Date.now()
  const base = { messageID: message.info.id, sessionID }
  yield* Effect.forEach(rows, (row) => {
    if (row.part.type === "message")
      return sessions.updatePart({
        ...base,
        id: PartID.ascending(),
        type: "text",
        text: row.part.text || row.part.refusal || "",
        time: { start: now, end: now },
      } satisfies SessionV1.TextPart)
    if (row.part.type === "reasoning")
      return sessions.updatePart({
        ...base,
        id: PartID.ascending(),
        type: "reasoning",
        text: row.part.text,
        time: { start: now, end: now },
      } satisfies SessionV1.ReasoningPart)
    if (row.part.type !== "tool_call" || known.has(row.part.tool_use_id)) return Effect.void
    const call = parseCall(row.part.tool_use_id, row.part.tool_name, row.part.arguments)
    // It was never run here. The next request answers it with this error instead of Twigg's generic tombstone.
    return sessions.updatePart({
      ...base,
      id: PartID.ascending(),
      type: "tool",
      tool: call.name,
      callID: call.id,
      state: {
        status: "error",
        input: call.input !== null && typeof call.input === "object" ? (call.input as Record<string, unknown>) : {},
        error: "Tool execution aborted",
        metadata: { interrupted: true },
        time: { start: now, end: now },
      },
    } satisfies SessionV1.ToolPart)
  })
})

// The install ID tells machines apart. The session ID alone can't: an imported session reuses its origin's ID.
export function tag(install: string, sessionID: string) {
  return { install, session_id: sessionID }
}

function landed(error: unknown): error is TwiggClient.ConflictError & { readonly runID: string } {
  return isTwiggError(error) && error._tag === "TwiggConflictError" && error.reason === "idempotency" && !!error.runID
}

function isTwiggError(error: unknown): error is TwiggClient.Error | TwiggPending.TwiggApiPending {
  return (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    typeof error._tag === "string" &&
    error._tag.startsWith("Twigg")
  )
}

// Twigg error codes worth another try (context/twigg/api-guide.md, "Errors").
const TRANSIENT = new Set(["external_service_error", "internal_error", "unavailable", "rate_limited"])

function toAPIError(error: TwiggClient.Error | TwiggPending.TwiggApiPending, started = false) {
  if (error._tag === "TwiggApiPending") return apiError(`${error.endpoint} isn't available on Twigg yet`, false)
  const status = "status" in error ? error.status : undefined
  // Before the run started, the request is re-posted with the same idempotency key. After it, only a run that failed
  // on the server for a passing reason is retried, through retry_of (see `failed_run`).
  const retry = started
    ? error._tag === "TwiggStreamError" && TRANSIENT.has(error.code)
    : error._tag === "TwiggRateLimitedError" ||
      error._tag === "TwiggTransportError" ||
      (error._tag === "TwiggServerError" && (status === 502 || status === 503)) ||
      (error._tag === "TwiggConflictError" && error.reason === "busy")
  return apiError(message(error, started), retry, status)
}

function message(error: TwiggClient.Error, started: boolean) {
  if (error._tag === "TwiggPaymentRequiredError")
    return "Your Twigg balance can't cover this request. Top up at https://twigg.ai/dashboard"
  if (error._tag === "TwiggConflictError" && error.reason === "busy")
    return "Waiting for the previous response on this chat to finish"
  if (error._tag === "TwiggTransportError" && started)
    return "Lost the connection to Twigg mid-response. The response finishes on the server and is picked up before the next message."
  if (error._tag === "TwiggStreamError" || error._tag === "TwiggTransportError") return error.message
  return error.hint ? `${error.message} (${error.hint})` : error.message
}

function apiError(message: string, retryable: boolean, statusCode?: number) {
  return new SessionV1.APIError({ message, isRetryable: retryable, statusCode }).toObject()
}

export * as TwiggRuntime from "./runtime"

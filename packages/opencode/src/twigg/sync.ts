import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Context, Effect, Layer, Option, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import os from "os"
import { Config } from "@/config/config"
import { InstanceState } from "@/effect/instance-state"
import { Provider } from "@/provider/provider"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { isRecord } from "@/util/record"
import { TwiggClient } from "./client"
import { TwiggModels } from "./models"
import { TwiggNamespace } from "./namespace"
import { TwiggRuntime } from "./runtime"

// Keeps local sessions in line with Twigg, which holds the conversations. The local SQLite is a UI cache, so a chat
// with no local session (a wiped cache, or later another machine) gets one, and its history is imported on open.
export interface Interface {
  // Creates a stub session for each opencode chat in this project's namespace that has none here.
  readonly discover: () => Effect.Effect<void>
  // Before a session's messages are read: imports a stub's history, or catches up with parts written elsewhere.
  readonly load: (sessionID: SessionID) => Effect.Effect<void>
  // Deletes the Twigg chats of a session and its subagent sessions, before the session itself is removed.
  readonly forget: (sessionID: SessionID) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/TwiggSync") {}

const DISCOVER_EVERY = 60_000
const CATCH_UP_EVERY = 30_000
const MAX_PAGES = 50

const decodeSessionID = Schema.decodeUnknownOption(SessionID)
const decodeArguments = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const provider = yield* Provider.Service
    const status = yield* SessionStatus.Service
    const config = yield* Config.Service
    const fs = yield* FSUtil.Service
    const http = yield* HttpClient.HttpClient
    const recent = new Map<string, number>()

    const settings = Effect.gen(function* () {
      const twigg = (yield* provider.list())[TwiggModels.PROVIDER_ID]
      if (!twigg?.key) return undefined
      return {
        baseURL: typeof twigg.options.baseURL === "string" ? twigg.options.baseURL : TwiggClient.DEFAULT_BASE_URL,
        apiKey: twigg.key,
      }
    })
    const install = TwiggNamespace.installID().pipe(Effect.provideService(FSUtil.Service, fs))
    const due = (key: string, every: number) => {
      if (Date.now() - (recent.get(key) ?? 0) < every) return false
      recent.set(key, Date.now())
      return true
    }
    const exists = (sessionID: SessionID) =>
      sessions.get(sessionID).pipe(
        Effect.as(true),
        Effect.catch(() => Effect.succeed(false)),
      )
    const save = (sessionID: SessionID, state: TwiggRuntime.State) =>
      sessions.get(sessionID).pipe(
        Effect.orDie,
        Effect.flatMap((session) =>
          sessions.setMetadata({ sessionID, metadata: { ...session.metadata, twigg: state } }),
        ),
      )

    const discover = Effect.fn("TwiggSync.discover")(function* () {
      const current = yield* settings
      const ctx = yield* InstanceState.context
      if (!current || !due(ctx.directory, DISCOVER_EVERY)) return
      const namespace = TwiggNamespace.namespaceFor({
        projectID: ctx.project.id,
        directory: ctx.directory,
        profile: yield* install,
        device: (yield* config.get()).twigg?.device ?? os.hostname(),
      })
      const chats = yield* listChats(current, namespace)
      // Only chats opencode created carry a session ID. Reusing it keeps the chat and the session matched for good.
      const sessionOf = new Map(
        chats.flatMap((chat) => Option.toArray(sessionIDOf(chat)).map((id) => [chat.id, id] as const)),
      )
      // Parents first, so a subagent session can point at its parent.
      const ordered = chats.toSorted(
        (a, b) => Number(parentChat(a) !== undefined) - Number(parentChat(b) !== undefined),
      )
      const created = yield* Effect.forEach(ordered, (chat) =>
        Effect.gen(function* () {
          const id = sessionOf.get(chat.id)
          if (!id || (yield* exists(id))) return 0
          const parent = sessionOf.get(parentChat(chat) ?? "")
          yield* sessions.create({
            id,
            parentID: parent && (yield* exists(parent)) ? parent : undefined,
            title: chat.title ?? "Twigg chat",
            metadata: {
              twigg: {
                chat_id: chat.id,
                namespace: chat.namespace ?? namespace,
                stub: true,
              } satisfies TwiggRuntime.State,
            },
          })
          return 1
        }),
      )
      const total = created.reduce((sum: number, item) => sum + item, 0)
      if (total > 0) yield* Effect.logInfo("found twigg chats without a local session", { created: total })
    })

    const load = Effect.fn("TwiggSync.load")(function* (sessionID: SessionID) {
      const session = yield* sessions.get(sessionID).pipe(Effect.catch(() => Effect.succeed(undefined)))
      const state = Option.getOrUndefined(TwiggRuntime.decodeState(session?.metadata?.twigg))
      const current = yield* settings
      if (!session || !state || !current) return
      // A running turn owns the session; the next open catches up.
      if ((yield* status.get(sessionID)).type !== "idle") return
      if (!state.stub && !due(sessionID, CATCH_UP_EVERY)) return
      const local = yield* sessions.messages({ sessionID }).pipe(Effect.orDie)
      // Sessions from before this sync existed, or a stub that was prompted before it was opened: take the chat as
      // it stands now and only catch up from here.
      if (state.synced_ordinal === undefined && (!state.stub || local.length > 0)) {
        const newest = yield* TwiggClient.request(current, TwiggClient.HistoryPage, {
          method: "GET",
          path: `/chats/${state.chat_id}/history`,
          query: { limit: 1 },
        })
        yield* save(sessionID, { ...state, stub: undefined, synced_ordinal: newest.last_ordinal ?? undefined })
        return
      }
      // Local messages the chat hasn't received yet go out with the next turn. Importing before then would mix the
      // two, so catching up waits until they're sent.
      const settled =
        state.cursor_message_id === undefined ? local.length === 0 : local.at(-1)?.info.id === state.cursor_message_id
      if (!settled) return
      const rows =
        state.synced_ordinal === undefined
          ? yield* history(current, state.chat_id)
          : yield* after(current, state.chat_id, state.synced_ordinal)
      // A stub reuses its origin's session ID, so on a full import every row counts. On catch-up, this machine's own
      // parts are already local.
      const mine = yield* install
      const foreign = state.stub
        ? rows
        : rows.filter((row) => !(isRecord(row.user_metadata) && row.user_metadata.install === mine))
      const ctx = yield* InstanceState.context
      const messages = build(foreign, {
        sessionID,
        parentID: local.findLast((message) => message.info.role === "user")?.info.id,
        model: session.model
          ? { providerID: session.model.providerID, modelID: session.model.id }
          : { providerID: TwiggModels.PROVIDER_ID, modelID: ModelV2.ID.make(TwiggModels.DEFAULT_MODELS[0]) },
        agent: session.agent ?? "build",
        path: { cwd: session.directory, root: ctx.worktree },
        // Messages are listed by creation time, so imports must not land before what's already here.
        after: local.at(-1)?.info.time.created,
      })
      yield* Effect.forEach(messages, (message) =>
        sessions.updateMessage(message.info).pipe(Effect.andThen(Effect.forEach(message.parts, sessions.updatePart))),
      )
      // Imported parts are already in the chat, so the cursor moves past them.
      yield* save(sessionID, {
        ...state,
        stub: undefined,
        synced_ordinal: rows.at(-1)?.ordinal ?? state.synced_ordinal,
        cursor_message_id: messages.at(-1)?.info.id ?? state.cursor_message_id,
      })
      if (messages.length > 0) yield* Effect.logInfo("imported twigg history", { sessionID, messages: messages.length })
    })

    const forget = Effect.fn("TwiggSync.forget")(function* (sessionID: SessionID) {
      const current = yield* settings
      if (!current) return
      const chats = yield* chatsOf(sessionID)
      yield* Effect.forEach(
        chats,
        (chatID) =>
          TwiggClient.request(current, Schema.Unknown, { method: "DELETE", path: `/chats/${chatID}` }).pipe(
            Effect.catchTag("TwiggNotFoundError", () => Effect.void),
          ),
        { concurrency: 4, discard: true },
      )
    })

    const chatsOf: (sessionID: SessionID) => Effect.Effect<string[]> = Effect.fnUntraced(function* (sessionID) {
      const session = yield* sessions.get(sessionID).pipe(Effect.catch(() => Effect.succeed(undefined)))
      const own = Option.toArray(TwiggRuntime.decodeState(session?.metadata?.twigg)).map((state) => state.chat_id)
      const kids = yield* sessions.children(sessionID)
      const nested = yield* Effect.forEach(kids, (kid) => chatsOf(kid.id))
      return [...own, ...nested.flat()]
    })

    const withHttp = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.provideService(HttpClient.HttpClient, http),
        // Twigg being unreachable must never break listing or opening sessions.
        Effect.catchCause((cause) => Effect.logWarning("twigg sync failed", { cause })),
      )

    return Service.of({
      discover: () => withHttp(discover()),
      load: (sessionID) => withHttp(load(sessionID)),
      forget: (sessionID) => withHttp(forget(sessionID)),
    })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [Session.node, Provider.node, SessionStatus.node, Config.node, FSUtil.node, httpClient],
})

// Rebuilds local messages from ledger rows. Each prompt starts a user message; consecutive model rows form one
// assistant message, as one opencode step does; tool results complete the matching tool part. There are no diffs,
// snapshots, timings or costs.
export function build(
  rows: readonly TwiggClient.HistoryRow[],
  input: {
    sessionID: SessionID
    // The user message that model rows at the start answer, when catching up mid-conversation.
    parentID?: MessageID
    model: { providerID: SessionV1.User["model"]["providerID"]; modelID: SessionV1.User["model"]["modelID"] }
    agent: string
    path: { cwd: string; root: string }
    // The newest local message's time; imported messages are never dated before it.
    after?: number
  },
) {
  const messages: SessionV1.WithParts[] = []
  const calls = new Map<string, SessionV1.ToolPart>()
  const state = { parentID: input.parentID, assistant: undefined as SessionV1.WithParts | undefined }
  const base = (messageID: MessageID) => ({ id: PartID.ascending(), messageID, sessionID: input.sessionID })

  rows.forEach((row) => {
    const time = Math.max(Date.parse(row.created_at) || Date.now(), input.after ?? 0)
    const part = row.part
    if (part.type === "prompt") {
      state.assistant = undefined
      const id = MessageID.ascending()
      const media = (row.media ?? []).map((item) => `[Attached ${item.mime}: ${item.filename ?? "file"}]`)
      messages.push({
        info: {
          id,
          sessionID: input.sessionID,
          role: "user",
          time: { created: time },
          agent: input.agent,
          model: input.model,
        },
        parts: [{ ...base(id), type: "text", text: [part.text, ...media].filter((text) => text !== "").join("\n\n") }],
      })
      state.parentID = id
      return
    }
    if (part.type === "tool_result") {
      state.assistant = undefined
      const call = calls.get(part.tool_use_id)
      if (!call || call.state.status === "pending") return
      const start = call.state.time.start
      call.state =
        part.is_error || part.tombstone_reason
          ? {
              status: "error",
              input: call.state.input,
              error: part.tombstone_reason ? `Tool call was not executed (${part.tombstone_reason})` : part.text,
              time: { start, end: time },
            }
          : {
              status: "completed",
              input: call.state.input,
              output: part.text,
              title: part.tool_name,
              metadata: {},
              time: { start, end: time },
            }
      return
    }
    const assistant = state.assistant ?? openAssistant(time)
    const messageID = assistant.info.id
    if (part.type === "message") {
      const text = part.text || part.refusal || ""
      if (text) assistant.parts.push({ ...base(messageID), type: "text", text, time: { start: time, end: time } })
      return
    }
    if (part.type === "reasoning") {
      if (part.text)
        assistant.parts.push({
          ...base(messageID),
          type: "reasoning",
          text: part.text,
          time: { start: time, end: time },
        })
      return
    }
    const args = Option.getOrUndefined(decodeArguments(part.arguments))
    const tool: SessionV1.ToolPart = {
      ...base(messageID),
      type: "tool",
      tool: part.tool_name,
      callID: part.tool_use_id,
      // Until a result row arrives, the call is still open on the server. The next turn answers it with this error.
      state: {
        status: "error",
        input: isRecord(args) ? args : {},
        error: "No result was recorded for this tool call",
        time: { start: time, end: time },
      },
    }
    calls.set(part.tool_use_id, tool)
    assistant.parts.push(tool)
    if (assistant.info.role === "assistant") assistant.info.finish = "tool-calls"
  })
  return messages

  function openAssistant(time: number) {
    const id = MessageID.ascending()
    const message: SessionV1.WithParts = {
      info: {
        id,
        sessionID: input.sessionID,
        role: "assistant",
        // A reply with no prompt before it (history that starts mid-turn) points at itself.
        parentID: state.parentID ?? id,
        modelID: input.model.modelID,
        providerID: input.model.providerID,
        mode: input.agent,
        agent: input.agent,
        path: input.path,
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        time: { created: time, completed: time },
        finish: "stop",
      },
      parts: [],
    }
    messages.push(message)
    state.assistant = message
    return message
  }
}

function sessionIDOf(chat: TwiggClient.ChatSummary) {
  return isRecord(chat.user_metadata) ? decodeSessionID(chat.user_metadata.session_id) : Option.none()
}

function parentChat(chat: TwiggClient.ChatSummary) {
  const parent = isRecord(chat.user_metadata) ? chat.user_metadata.parent_chat_id : undefined
  return typeof parent === "string" ? parent : undefined
}

// Chats in the namespace and its subtree (subagent chats included), newest first.
const listChats = Effect.fnUntraced(function* (settings: TwiggClient.Settings, namespace: string) {
  const chats: TwiggClient.ChatSummary[] = []
  const cursor = { after: undefined as string | undefined, pages: 0 }
  while (cursor.pages++ < MAX_PAGES) {
    const page = yield* TwiggClient.request(settings, TwiggClient.ChatPage, {
      method: "GET",
      path: "/chats",
      query: { namespace, limit: 100, after_id: cursor.after },
    })
    chats.push(...page.data)
    if (!page.has_more || !page.last_id) break
    cursor.after = page.last_id
  }
  return chats
})

// The whole ledger, oldest first. Without a cursor Twigg returns the newest page, so this pages backwards.
const history = Effect.fnUntraced(function* (settings: TwiggClient.Settings, chatID: string) {
  const pages: TwiggClient.HistoryRow[][] = []
  const cursor = { before: undefined as number | undefined, count: 0 }
  while (cursor.count++ < MAX_PAGES) {
    const page = yield* TwiggClient.request(settings, TwiggClient.HistoryPage, {
      method: "GET",
      path: `/chats/${chatID}/history`,
      query: { limit: 100, before_ordinal: cursor.before },
    })
    pages.unshift([...page.data])
    if (!page.has_more || page.first_ordinal === null) break
    cursor.before = page.first_ordinal
  }
  return pages.flat()
})

// Rows after an ordinal, oldest first.
const after = Effect.fnUntraced(function* (settings: TwiggClient.Settings, chatID: string, ordinal: number) {
  const rows: TwiggClient.HistoryRow[] = []
  const cursor = { after: ordinal, count: 0 }
  while (cursor.count++ < MAX_PAGES) {
    const page = yield* TwiggClient.request(settings, TwiggClient.HistoryPage, {
      method: "GET",
      path: `/chats/${chatID}/history`,
      query: { limit: 100, after_ordinal: cursor.after },
    })
    rows.push(...page.data)
    if (!page.has_more || page.last_ordinal === null) break
    cursor.after = page.last_ordinal
  }
  return rows
})

export * as TwiggSync from "./sync"

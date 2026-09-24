import { describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Effect, Layer, Option } from "effect"
import { HttpClient, HttpClientResponse, UrlParams } from "effect/unstable/http"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Provider } from "@/provider/provider"
import { MessageID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { TwiggModels } from "../../src/twigg/models"
import { TwiggNamespace } from "../../src/twigg/namespace"
import { TwiggRuntime } from "../../src/twigg/runtime"
import { TwiggSync } from "../../src/twigg/sync"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ProviderTest } from "../fake/provider"
import { provideTmpdirInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const model = ProviderTest.model({ id: ModelV2.ID.make("gpt-6-luna"), providerID: TwiggModels.PROVIDER_ID })
const twigg = ProviderTest.fake({
  model,
  info: ProviderTest.info(
    { id: TwiggModels.PROVIDER_ID, key: "tw_test", options: { baseURL: "https://twigg.test/api/v1" } },
    model,
  ),
})

// Each test sets the routes it needs; a key is "METHOD /path", answered in order.
const server = {
  routes: {} as Record<string, Array<(query: Record<string, string>) => Response>>,
  calls: [] as string[],
}
const fakeHttp = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.sync(() => {
      const key = `${request.method} ${new URL(request.url).pathname.replace("/api/v1", "")}`
      const query = Object.fromEntries(
        ["namespace", "before_ordinal", "after_ordinal", "limit"].flatMap((name) =>
          Option.toArray(UrlParams.getFirst(request.urlParams, name)).map((value) => [name, value]),
        ),
      )
      server.calls.push(Object.keys(query).length > 0 ? `${key}?${new URLSearchParams(query)}` : key)
      const next = server.routes[key]?.shift()
      return HttpClientResponse.fromWeb(
        request,
        next ? next(query) : Response.json({ error: { code: "not_found", message: key } }, { status: 404 }),
      )
    }),
  ),
)
function script(routes: Record<string, Array<(query: Record<string, string>) => Response>>) {
  server.routes = routes
  server.calls = []
}

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      TwiggSync.node,
      Session.node,
      SessionProjector.node,
      Database.node,
      EventV2Bridge.node,
      CrossSpawnSpawner.node,
      FSUtil.node,
    ]),
    [
      [Provider.node, twigg.layer],
      [httpClient, fakeHttp],
    ],
  ),
)

const at = "2026-09-24T10:00:00Z"
const row = (ordinal: number, part: Record<string, unknown>, extra: Record<string, unknown> = {}) => ({
  ordinal,
  created_at: at,
  role: part.type === "prompt" || part.type === "tool_result" ? "user" : "assistant",
  part,
  ...extra,
})
const page = (rows: unknown[], extra: { has_more?: boolean } = {}) =>
  Response.json({
    data: rows,
    first_ordinal: (rows[0] as { ordinal: number } | undefined)?.ordinal ?? null,
    last_ordinal: (rows.at(-1) as { ordinal: number } | undefined)?.ordinal ?? null,
    has_more: extra.has_more ?? false,
  })

const texts = (messages: readonly { info: { role: string }; parts: readonly { type: string; text?: string }[] }[]) =>
  messages.map((message) => `${message.info.role}: ${message.parts.map((part) => part.text ?? part.type).join(" | ")}`)

describe("twigg sync", () => {
  it.live(
    "discovers chats without a local session, parents first",
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const parentID = SessionID.make("ses_parent_from_other_cache")
        const childID = SessionID.make("ses_child_from_other_cache")
        script({
          "GET /chats": [
            () =>
              Response.json({
                data: [
                  {
                    id: "chat_child",
                    namespace: "ns/a/explore",
                    title: "Explore",
                    created_at: at,
                    user_metadata: { session_id: childID, parent_chat_id: "chat_parent" },
                  },
                  {
                    id: "chat_parent",
                    namespace: "ns",
                    title: "Fix the bug",
                    created_at: at,
                    user_metadata: { session_id: parentID },
                  },
                  { id: "chat_api", namespace: "ns", title: "Made elsewhere", created_at: at, user_metadata: {} },
                ],
                last_id: "chat_api",
                has_more: false,
              }),
          ],
        })
        const sync = yield* TwiggSync.Service
        const sessions = yield* Session.Service
        yield* sync.discover()

        const parent = yield* sessions.get(parentID)
        const child = yield* sessions.get(childID)
        expect(parent.title).toBe("Fix the bug")
        expect(parent.metadata?.twigg).toEqual({ chat_id: "chat_parent", namespace: "ns", stub: true })
        expect(child.parentID).toBe(parentID)
        expect(server.calls[0]).toStartWith(`GET /chats?namespace=${encodeURIComponent(TwiggNamespace.ROOT + "/")}`)
        // Throttled: a second listing right away doesn't ask Twigg again.
        yield* sync.discover()
        expect(server.calls.length).toBe(1)
      }),
    ),
  )

  it.live(
    "imports a stub's whole history when it is opened",
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const sync = yield* TwiggSync.Service
        const session = yield* sessions.create({
          metadata: { twigg: { chat_id: "chat_1", namespace: "ns", stub: true } satisfies TwiggRuntime.State },
        })
        script({
          // Newest page first, then the older one.
          "GET /chats/chat_1/history": [
            () =>
              page(
                [
                  row(4, {
                    type: "tool_result",
                    tool_use_id: "call_1",
                    tool_name: "read",
                    text: "file body",
                    is_error: false,
                  }),
                  row(6, { type: "message", text: "It says hello." }),
                ],
                { has_more: true },
              ),
            () =>
              page([
                row(
                  1,
                  { type: "prompt", text: "What's in a.ts?" },
                  { media: [{ mime: "image/png", filename: "shot.png" }] },
                ),
                row(2, { type: "reasoning", text: "Let me read it" }),
                row(3, { type: "tool_call", tool_use_id: "call_1", tool_name: "read", arguments: '{"path":"a.ts"}' }),
              ]),
          ],
        })
        yield* sync.load(session.id)

        const messages = yield* sessions.messages({ sessionID: session.id })
        expect(texts(messages)).toEqual([
          "user: What's in a.ts?\n\n[Attached image/png: shot.png]",
          "assistant: Let me read it | tool",
          "assistant: It says hello.",
        ])
        const tool = messages[1].parts.find((part) => part.type === "tool")
        expect(tool?.type === "tool" && tool.state).toMatchObject({
          status: "completed",
          input: { path: "a.ts" },
          output: "file body",
        })
        expect(messages[1].info.role === "assistant" && messages[1].info.finish).toBe("tool-calls")
        expect(server.calls).toEqual([
          "GET /chats/chat_1/history?limit=100",
          "GET /chats/chat_1/history?before_ordinal=4&limit=100",
        ])
        expect((yield* sessions.get(session.id)).metadata?.twigg).toEqual({
          chat_id: "chat_1",
          namespace: "ns",
          synced_ordinal: 6,
          cursor_message_id: messages[2].info.id,
        })
      }),
    ),
  )

  it.live(
    "catches up with parts written elsewhere, skipping its own",
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const sync = yield* TwiggSync.Service
        const fs = yield* FSUtil.Service
        const install = yield* TwiggNamespace.installID().pipe(Effect.provideService(FSUtil.Service, fs))
        const session = yield* sessions.create({})
        const mine = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          sessionID: session.id,
          role: "user",
          time: { created: Date.now() },
          agent: "build",
          model: { providerID: model.providerID, modelID: model.id },
        })
        yield* sessions.setMetadata({
          sessionID: session.id,
          metadata: {
            twigg: {
              chat_id: "chat_1",
              namespace: "ns",
              synced_ordinal: 4,
              cursor_message_id: mine.id,
            } satisfies TwiggRuntime.State,
          },
        })
        script({
          "GET /chats/chat_1/history": [
            () =>
              page([
                row(
                  5,
                  { type: "message", text: "my own reply" },
                  { user_metadata: { install, session_id: session.id } },
                ),
                row(7, { type: "prompt", text: "from the laptop" }, { user_metadata: { install: "other" } }),
                row(8, { type: "message", text: "answered there" }, { user_metadata: { install: "other" } }),
              ]),
          ],
        })
        yield* sync.load(session.id)

        const messages = yield* sessions.messages({ sessionID: session.id })
        expect(texts(messages)).toEqual(["user: ", "user: from the laptop", "assistant: answered there"])
        expect(server.calls).toEqual(["GET /chats/chat_1/history?after_ordinal=4&limit=100"])
        expect((yield* sessions.get(session.id)).metadata?.twigg).toMatchObject({
          synced_ordinal: 8,
          cursor_message_id: messages[2].info.id,
        })
      }),
    ),
  )

  it.live(
    "waits to catch up while local messages are still unsent",
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const sync = yield* TwiggSync.Service
        const session = yield* sessions.create({})
        yield* sessions.updateMessage({
          id: MessageID.ascending(),
          sessionID: session.id,
          role: "user",
          time: { created: Date.now() },
          agent: "build",
          model: { providerID: model.providerID, modelID: model.id },
        })
        yield* sessions.setMetadata({
          sessionID: session.id,
          metadata: {
            twigg: { chat_id: "chat_1", namespace: "ns", synced_ordinal: 4, cursor_message_id: "msg_older" },
          },
        })
        script({})
        yield* sync.load(session.id)
        expect(server.calls).toEqual([])
      }),
    ),
  )

  it.live(
    "deletes the chats of a session and its subagents",
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const sync = yield* TwiggSync.Service
        const parent = yield* sessions.create({ metadata: { twigg: { chat_id: "chat_p", namespace: "ns" } } })
        yield* sessions.create({ parentID: parent.id, metadata: { twigg: { chat_id: "chat_c", namespace: "ns/a/x" } } })
        script({
          "DELETE /chats/chat_p": [() => new Response(null, { status: 204 })],
          "DELETE /chats/chat_c": [
            () => Response.json({ error: { code: "not_found", message: "gone" } }, { status: 404 }),
          ],
        })
        yield* sync.forget(parent.id)
        expect(server.calls.toSorted()).toEqual(["DELETE /chats/chat_c", "DELETE /chats/chat_p"])
      }),
    ),
  )
})

describe("twigg history import", () => {
  test("closes tombstoned calls and leaves open ones as errors", () => {
    const messages = TwiggSync.build(
      [
        row(1, { type: "prompt", text: "go" }),
        row(2, { type: "tool_call", tool_use_id: "a", tool_name: "bash", arguments: "{}" }),
        row(3, { type: "tool_call", tool_use_id: "b", tool_name: "bash", arguments: "not json" }),
        row(4, {
          type: "tool_result",
          tool_use_id: "a",
          tool_name: "bash",
          text: "Tool call was not executed.",
          is_error: false,
          tombstone_reason: "abandoned",
        }),
      ] as never,
      {
        sessionID: SessionID.make("ses_x"),
        model: { providerID: model.providerID, modelID: model.id },
        agent: "build",
        path: { cwd: "/", root: "/" },
      },
    )
    const states = messages[1].parts.map((part) => (part.type === "tool" ? [part.state.status, part.state.input] : []))
    expect(states).toEqual([
      ["error", {}],
      ["error", {}],
    ])
    const errors = messages[1].parts.map(
      (part) => part.type === "tool" && part.state.status === "error" && part.state.error,
    )
    expect(errors).toEqual(["Tool call was not executed (abandoned)", "No result was recorded for this tool call"])
  })
})

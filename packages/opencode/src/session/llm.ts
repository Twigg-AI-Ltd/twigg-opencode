import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Context, Effect, Layer } from "effect"
import * as Stream from "effect/Stream"
import { HttpClient } from "effect/unstable/http"
import type { ModelMessage, Tool } from "ai"
import type { LLMEvent } from "@opencode-ai/llm"
import os from "os"
import { Provider } from "@/provider/provider"
import { Config } from "@/config/config"
import type { Agent } from "@/agent/agent"
import { Plugin } from "@/plugin"
import { SessionID } from "@/session/schema"
import { Auth } from "@/auth"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { TwiggModels } from "@/twigg/models"
import { TwiggRuntime } from "@/twigg/runtime"
import { LLMRequestPrep } from "./llm/request"
import { Session } from "./session"
import { Instruction } from "./instruction"

export type StreamInput = {
  user: SessionV1.User
  sessionID: string
  parentSessionID?: string
  model: Provider.Model
  agent: Agent.Info
  permission?: PermissionV1.Ruleset
  system: string[]
  messages: ModelMessage[]
  small?: boolean
  tools: Record<string, Tool>
  retries?: number
  toolChoice?: "auto" | "required" | "none"
  // Set by the session loop. The Twigg runtime sends only what's new since its cursor, so it needs the local messages
  // (with their IDs) and the assistant message being filled. Calls without them are side calls.
  history?: SessionV1.WithParts[]
  assistantID?: string
}

export type StreamRequest = StreamInput & {
  abort: AbortSignal
}

export interface Interface {
  readonly stream: (input: StreamInput) => Stream.Stream<LLMEvent, unknown>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/LLM") {}

export const use = serviceUse(Service)

const live: Layer.Layer<
  Service,
  never,
  | Auth.Service
  | Config.Service
  | Provider.Service
  | Plugin.Service
  | RuntimeFlags.Service
  | Session.Service
  | Instruction.Service
  | FSUtil.Service
  | HttpClient.HttpClient
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const auth = yield* Auth.Service
    const config = yield* Config.Service
    const provider = yield* Provider.Service
    const plugin = yield* Plugin.Service
    const flags = yield* RuntimeFlags.Service
    const sessions = yield* Session.Service
    const fs = yield* FSUtil.Service
    const http = yield* HttpClient.HttpClient
    const instruction = yield* Instruction.Service
    const published = new Map<string, string>()

    // Every model runs through Twigg, which keeps the conversation server-side.
    const run = Effect.fn("LLM.run")(function* (input: StreamRequest) {
      yield* Effect.logInfo("stream", {
        providerID: input.model.providerID,
        modelID: input.model.id,
        "session.id": input.sessionID,
        small: (input.small ?? false).toString(),
        agent: input.agent.name,
        mode: input.agent.mode,
      })
      if (input.model.providerID !== TwiggModels.PROVIDER_ID)
        return yield* Effect.die(new Error(`Only Twigg models are supported, not ${input.model.providerID}`))
      const [cfg, item, info] = yield* Effect.all(
        [config.get(), provider.getProvider(input.model.providerID), auth.get(input.model.providerID)],
        { concurrency: "unbounded" },
      )
      const prepared = yield* LLMRequestPrep.prepare({
        ...input,
        provider: item,
        auth: info,
        plugin,
        flags,
        isWorkflow: false,
      })
      const settings = TwiggModels.settings(item)
      if (!settings) return yield* Effect.die(new Error(`Set ${TwiggModels.ENV_KEY} or log in to Twigg first`))
      const variant = input.user.model.variant ? input.model.variants?.[input.user.model.variant] : undefined
      // AGENTS.md and agent prompts are published to the chat's namespaces. The rest of the system text changes per
      // machine or per turn, so it goes to the chat as a context block instead.
      const split = yield* instruction.levels().pipe(Effect.orDie)
      const files = new Set([...split.global, ...split.project])
      const child = input.parentSessionID !== undefined
      const stream =
        input.small || !input.history || !input.assistantID
          ? TwiggRuntime.respond({
              settings,
              model: input.model,
              system: prepared.system,
              messages: input.messages,
              maxTokens: prepared.params.maxOutputTokens,
            })
          : TwiggRuntime.stream({
              settings,
              model: input.model,
              assistantID: input.assistantID,
              history: input.history,
              tools: prepared.tools,
              messages: input.messages,
              maxTokens: prepared.params.maxOutputTokens,
              reasoningEffort: typeof variant?.reasoning_effort === "string" ? variant.reasoning_effort : undefined,
              abort: input.abort,
              instructions: { ...split, agent: child ? input.agent.prompt : undefined },
              // A primary agent's own prompt can't have a namespace (build and plan share one chat), so it rides
              // along in the context block.
              context: [
                ...(!child && input.agent.prompt ? [input.agent.prompt] : []),
                ...input.system.filter((item) => !files.has(item)),
                ...(input.user.system ? [input.user.system] : []),
              ].join("\n\n"),
              published,
              chat: yield* TwiggRuntime.sessionChat({
                sessionID: SessionID.make(input.sessionID),
                parentSessionID: input.parentSessionID ? SessionID.make(input.parentSessionID) : undefined,
                agent: input.agent.name,
                device: cfg.twigg?.device ?? os.hostname(),
              }).pipe(Effect.provideService(Session.Service, sessions), Effect.provideService(FSUtil.Service, fs)),
            })
      return stream.pipe(Stream.provideService(HttpClient.HttpClient, http))
    })

    const stream: Interface["stream"] = (input) =>
      Stream.scoped(
        Stream.unwrap(
          Effect.gen(function* () {
            const ctrl = yield* Effect.acquireRelease(
              Effect.sync(() => new AbortController()),
              (ctrl) => Effect.sync(() => ctrl.abort()),
            )
            return yield* run({ ...input, abort: ctrl.signal })
          }),
        ),
      )

    return Service.of({ stream })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: live,
  deps: [
    Auth.node,
    Config.node,
    Provider.node,
    Plugin.node,
    RuntimeFlags.node,
    Session.node,
    Instruction.node,
    FSUtil.node,
    httpClient,
  ],
})

export * as LLM from "./llm"

import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { RuntimeFlags } from "@/effect/runtime-flags"
import { Permission } from "@/permission"
import type { Agent } from "@/agent/agent"
import type { Provider } from "@/provider/provider"
import { Effect, Record } from "effect"
import type { Tool } from "ai"
import type { Plugin } from "@/plugin"
import { mergeDeep } from "remeda"

// The default output ceiling when OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX isn't set.
export const OUTPUT_TOKEN_MAX = 32_000

type PrepareInput = {
  readonly user: SessionV1.User
  readonly sessionID: string
  readonly model: Provider.Model
  readonly agent: Agent.Info
  readonly permission?: PermissionV1.Ruleset
  readonly system: string[]
  readonly small?: boolean
  readonly tools: Record<string, Tool>
  readonly provider: Provider.Info
  readonly plugin: Plugin.Interface
  readonly flags: RuntimeFlags.Info
}

export type Prepared = {
  readonly system: string[]
  readonly tools: Record<string, Tool>
  readonly params: {
    readonly maxOutputTokens: number
    readonly options: Record<string, any>
  }
}

// Builds what a Twigg request needs from opencode's session input: the system text (plugins may rewrite it), the
// tools the agent and session permissions allow, and the output ceiling and options (plugins may adjust them).
export const prepare = Effect.fn("LLMRequestPrep.prepare")(function* (input: PrepareInput) {
  const system = [
    [
      ...(input.agent.prompt ? [input.agent.prompt] : []),
      ...input.system,
      ...(input.user.system ? [input.user.system] : []),
    ]
      .filter((x) => x)
      .join("\n"),
  ]
  yield* input.plugin.trigger(
    "experimental.chat.system.transform",
    { sessionID: input.sessionID, model: input.model },
    { system },
  )

  const variant =
    !input.small && input.model.variants && input.user.model.variant
      ? input.model.variants[input.user.model.variant]
      : {}
  const options = mergeDeep(mergeDeep(input.model.options, input.agent.options ?? {}), variant ?? {}) as Record<
    string,
    any
  >
  const params = yield* input.plugin.trigger(
    "chat.params",
    {
      sessionID: input.sessionID,
      agent: input.agent.name,
      model: input.model,
      provider: input.provider,
      message: input.user,
    },
    {
      temperature: undefined,
      topP: undefined,
      topK: undefined,
      maxOutputTokens: maxOutputTokens(input.model, input.flags.outputTokenMax),
      options,
    },
  )

  return {
    system,
    tools: Object.fromEntries(Object.entries(resolveTools(input)).toSorted(([a], [b]) => a.localeCompare(b))),
    params: {
      maxOutputTokens: params.maxOutputTokens ?? maxOutputTokens(input.model, input.flags.outputTokenMax),
      options: params.options,
    },
  } satisfies Prepared
})

export function maxOutputTokens(model: Provider.Model, outputTokenMax = OUTPUT_TOKEN_MAX) {
  return Math.min(model.limit.output || outputTokenMax, outputTokenMax)
}

function resolveTools(input: Pick<PrepareInput, "tools" | "agent" | "permission" | "user">) {
  const disabled = Permission.disabled(
    Object.keys(input.tools),
    Permission.merge(input.agent.permission, input.permission ?? []),
  )
  return Record.filter(input.tools, (_, k) => input.user.tools?.[k] !== false && !disabled.has(k))
}

export * as LLMRequestPrep from "./request"

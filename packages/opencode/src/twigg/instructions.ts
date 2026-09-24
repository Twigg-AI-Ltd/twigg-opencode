import { Hash } from "@opencode-ai/core/util/hash"
import { Effect, Schema } from "effect"
import BASE from "@/session/prompt/default.txt"
import { TwiggClient } from "./client"

// Twigg rejects a larger body per level.
export const MAX_BODY_BYTES = 131_072

export interface Sources {
  // The user's global AGENTS.md (or ~/.claude/CLAUDE.md).
  readonly global: readonly string[]
  // Project AGENTS.md/CLAUDE.md and config instructions.
  readonly project: readonly string[]
  // The subagent's own prompt, for chats under …/a/<agent>.
  readonly agent?: string
}

export interface Level {
  readonly namespace: string
  readonly mode: "append" | "replace"
  // Empty means the level has nothing of its own, so any active version there is withdrawn.
  readonly body: string
}

const Version = Schema.Struct({
  id: Schema.String,
  namespace: Schema.NullOr(Schema.String),
  is_active: Schema.Boolean,
})
const Instruction = Schema.Struct({ body: Schema.String, mode: Schema.NullOr(Schema.String) })

// The namespace path mirrors the instruction hierarchy, the way AGENTS.md scopes to a directory:
//   twigg-code/<install>          model-neutral base prompt + global AGENTS.md
//   …/p/<project> (or …/d/…)      project instructions
//   …/a/<agent>                   subagent prompt, replacing everything above (user, 2026-09-24). An agent prompt
//                                 replaces the base prompt today while AGENTS.md still applies, so the global and
//                                 project instructions are copied in.
export function levels(namespace: string, sources: Sources): Level[] {
  const agent = namespace.match(/^(.+)\/a\/[^/]+$/)
  const project = agent ? agent[1] : namespace
  return [
    { namespace: namespace.split("/").slice(0, 2).join("/"), mode: "append", body: join([BASE, ...sources.global]) },
    { namespace: project, mode: "append", body: join(sources.project) },
    ...(agent
      ? [
          {
            namespace,
            mode: "replace" as const,
            // An agent without its own prompt keeps the inherited base prompt, as it does today.
            body: sources.agent ? join([sources.agent, ...sources.global, ...sources.project]) : "",
          },
        ]
      : []),
  ]
}

// Publishes each level whose body differs from its active version. `published` remembers what this process already
// checked, so an unchanged level costs no requests. Another machine publishing in between is last-writer-wins.
export const ensure = Effect.fn("TwiggInstructions.ensure")(function* (
  settings: TwiggClient.Settings,
  levels: readonly Level[],
  published: Map<string, string>,
) {
  yield* Effect.forEach(levels, (level) => publish(settings, level, published), { concurrency: "unbounded" })
})

const publish = Effect.fnUntraced(function* (
  settings: TwiggClient.Settings,
  level: Level,
  published: Map<string, string>,
) {
  const body = yield* clip(level)
  const key = `${settings.baseURL} ${level.namespace}`
  const hash = Hash.fast(`${level.mode}\n${body}`)
  if (published.get(key) === hash) return
  const versions = yield* TwiggClient.request(settings, Schema.Array(Version), {
    method: "GET",
    path: "/config/instructions",
    query: { namespace: level.namespace },
  })
  const active = versions.find((version) => version.is_active && version.namespace === level.namespace)
  const current = active
    ? yield* TwiggClient.request(settings, Instruction, { method: "GET", path: `/config/instructions/${active.id}` })
    : undefined
  if (body === "" && active)
    yield* TwiggClient.request(settings, Schema.Unknown, {
      method: "DELETE",
      path: "/config/instructions/active",
      query: { namespace: level.namespace },
    })
  if (body !== "" && (current?.body !== body || current.mode !== level.mode)) {
    yield* Effect.logInfo("publishing twigg instructions", { namespace: level.namespace, mode: level.mode })
    yield* TwiggClient.request(settings, Schema.Unknown, {
      method: "POST",
      path: "/config/instructions",
      body: { namespace: level.namespace, mode: level.mode, body },
    })
  }
  published.set(key, hash)
})

function join(parts: readonly string[]) {
  return parts.filter((part) => part.trim() !== "").join("\n\n")
}

const clip = Effect.fnUntraced(function* (level: Level) {
  if (Buffer.byteLength(level.body) <= MAX_BODY_BYTES) return level.body
  yield* Effect.logWarning("twigg instructions too long, truncated", { namespace: level.namespace })
  const note = "\n\n[Instructions truncated: over Twigg's size limit]"
  // A cut through a multi-byte character decodes to U+FFFD (3 bytes), hence the small margin.
  return (
    Buffer.from(level.body)
      .subarray(0, MAX_BODY_BYTES - Buffer.byteLength(note) - 4)
      .toString() + note
  )
})

export * as TwiggInstructions from "./instructions"

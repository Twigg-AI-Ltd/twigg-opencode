import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Hash } from "@opencode-ai/core/util/hash"
import { Context, Duration, Effect, Layer, Option, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import path from "path"
import type { Provider } from "@/provider/provider"
import { TwiggClient } from "./client"

export const PROVIDER_ID = ProviderV2.ID.make("twigg")
export const ENV_KEY = "TWIGG_API_KEY"
// Sentinel for `api.npm`. It must never reach the AI SDK loader, which would try to install it from npm.
export const NPM = "twigg"
// Preferred default when nothing is configured or recently used; the first one the account has wins.
export const DEFAULT_MODELS = ["claude-sonnet-5", "gpt-6-luna"]
// Twigg's reasoning_effort values, lowest first.
export const EFFORTS = ["off", "low", "medium", "high", "x_high", "max"] as const

const Rates = Schema.Struct({
  input: Schema.String,
  output: Schema.String,
  // Null when the model has no prompt cache pricing, e.g. OpenAI models don't bill cache writes.
  cache_read: Schema.NullOr(Schema.String),
  cache_write: Schema.NullOr(Schema.String),
})

const MediaSlot = Schema.Struct({
  mime_types: Schema.Array(Schema.String),
  max_bytes_per_attachment: Schema.NullOr(Schema.Number),
  max_attachments_per_request: Schema.NullOr(Schema.Number),
})

// The subset of a GET /models entry that opencode uses. Prices are the caller's own, per million tokens.
export const Model = Schema.Struct({
  name: Schema.String,
  display_name: Schema.String,
  model_family: Schema.String,
  context_window: Schema.Number,
  max_output_tokens: Schema.Number,
  supports_reasoning: Schema.Boolean,
  supports_tools: Schema.Boolean,
  max_reasoning_effort: Schema.optional(Schema.NullOr(Schema.String)),
  max_tool_definitions: Schema.optional(Schema.NullOr(Schema.Number)),
  media: Schema.Struct({
    max_attachments_per_part: Schema.optional(Schema.NullOr(Schema.Number)),
    user: MediaSlot,
    tool_result: MediaSlot,
  }),
  rates: Rates,
  tiers: Schema.Array(
    Schema.Struct({ mode: Schema.String, metric: Schema.String, min: Schema.Number, ...Rates.fields }),
  ),
})
export type Model = typeof Model.Type

const decodeCache = Schema.decodeUnknownOption(Schema.Array(Model))
const decodeModel = Schema.decodeUnknownOption(Model)
const TTL = Duration.minutes(5)

export interface Interface {
  // Cached on disk for 5 minutes. Falls back to a stale cache, then to no models, when the API can't be reached.
  readonly get: (settings: TwiggClient.Settings) => Effect.Effect<readonly Model[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/TwiggModels") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const http = yield* HttpClient.HttpClient

    const get = Effect.fn("TwiggModels.get")(function* (settings: TwiggClient.Settings) {
      const file = path.join(Global.Path.cache, `twigg-models-${Hash.fast(settings.baseURL).slice(0, 8)}.json`)
      const cached = yield* fs.readJson(file).pipe(
        Effect.map((value) => Option.getOrUndefined(decodeCache(value))),
        Effect.catch(() => Effect.succeed(undefined)),
      )
      const stat = yield* fs.stat(file).pipe(Effect.catch(() => Effect.succeed(undefined)))
      const age = stat ? Date.now() - Option.getOrElse(stat.mtime, () => new Date(0)).getTime() : Infinity
      if (cached && age < Duration.toMillis(TTL)) return cached
      return yield* TwiggClient.request(settings, Schema.Array(Schema.Unknown), {
        method: "GET",
        path: "/models",
      }).pipe(
        Effect.timeout("10 seconds"),
        // One entry the schema doesn't expect shouldn't hide every other model.
        Effect.flatMap((items) => {
          const models = items.flatMap((item) => Option.toArray(decodeModel(item)))
          if (models.length === items.length) return Effect.succeed(models)
          return Effect.logWarning("skipped unreadable twigg models", { skipped: items.length - models.length }).pipe(
            Effect.as(models),
          )
        }),
        Effect.tap((models) => fs.writeWithDirs(file, JSON.stringify(models)).pipe(Effect.ignore)),
        Effect.catch((error) =>
          Effect.logWarning("failed to load twigg models", { error }).pipe(Effect.as(cached ?? [])),
        ),
        Effect.provideService(HttpClient.HttpClient, http),
      )
    })

    return Service.of({ get })
  }),
)

// Drops the cached model lists so the next load asks Twigg again.
export const clearCache = Effect.fn("TwiggModels.clearCache")(function* () {
  const fs = yield* FSUtil.Service
  const files = yield* fs.readDirectory(Global.Path.cache).pipe(Effect.catch(() => Effect.succeed([] as string[])))
  yield* Effect.forEach(
    files.filter((file) => file.startsWith("twigg-models-")),
    (file) => fs.remove(path.join(Global.Path.cache, file)).pipe(Effect.ignore),
  )
})

export const node = makeGlobalNode({ service: Service, layer, deps: [FSUtil.node, httpClient] })

export function preferredModel(models: Record<string, unknown>) {
  return DEFAULT_MODELS.find((id) => id in models)
}

// How to reach Twigg for a connected provider: the key comes from env or auth.json (`key`) or from config
// (`options.apiKey`).
export function settings(info: Provider.Info | undefined): TwiggClient.Settings | undefined {
  const apiKey = typeof info?.options.apiKey === "string" ? info.options.apiKey : info?.key
  if (!info || !apiKey) return undefined
  return {
    baseURL: typeof info.options.baseURL === "string" ? info.options.baseURL : TwiggClient.DEFAULT_BASE_URL,
    apiKey,
  }
}

export function toProvider(models: readonly Model[], baseURL: string): Provider.Info {
  return {
    id: PROVIDER_ID,
    name: "Twigg",
    source: "custom",
    env: [ENV_KEY],
    options: { baseURL },
    models: Object.fromEntries(models.map((model) => [model.name, toModel(model, baseURL)])),
  }
}

export function toModel(model: Model, baseURL: string): Provider.Model {
  const mimes = model.media.user.mime_types
  return {
    id: ModelV2.ID.make(model.name),
    providerID: PROVIDER_ID,
    api: { id: model.name, url: baseURL, npm: NPM },
    name: model.display_name,
    family: model.model_family,
    capabilities: {
      // Twigg has no temperature parameter.
      temperature: false,
      reasoning: model.supports_reasoning,
      attachment: mimes.length > 0,
      toolcall: model.supports_tools,
      input: {
        text: true,
        audio: mimes.some((mime) => mime.startsWith("audio/")),
        image: mimes.some((mime) => mime.startsWith("image/")),
        video: mimes.some((mime) => mime.startsWith("video/")),
        pdf: mimes.includes("application/pdf"),
      },
      output: { text: true, audio: false, image: false, video: false, pdf: false },
      interleaved: false,
    },
    cost: {
      ...rates(model.rates),
      // opencode applies a tier once context tokens exceed `size`; Twigg cliff tiers start at `min`.
      tiers: model.tiers
        .filter((tier) => tier.mode === "cliff" && tier.metric === "contexttokens")
        .map((tier) => ({ ...rates(tier), tier: { type: "context" as const, size: tier.min - 1 } })),
    },
    limit: { context: model.context_window, output: model.max_output_tokens },
    status: "active",
    // Read by the Twigg runtime (Phase 2) to check attachments before sending them.
    options: { twigg: { media: model.media, max_tool_definitions: model.max_tool_definitions ?? null } },
    headers: {},
    release_date: "",
    variants: variants(model),
  }
}

// A null max_reasoning_effort means Twigg publishes no cap; it clamps an effort that's too high with a warning.
function variants(model: Model) {
  if (!model.supports_reasoning) return {}
  const max = EFFORTS.findIndex((effort) => effort === model.max_reasoning_effort)
  return Object.fromEntries(
    EFFORTS.slice(0, max === -1 ? EFFORTS.length : max + 1).map((effort) => [effort, { reasoning_effort: effort }]),
  )
}

function rates(input: typeof Rates.Type) {
  return {
    input: Number(input.input),
    output: Number(input.output),
    cache: { read: Number(input.cache_read ?? 0), write: Number(input.cache_write ?? 0) },
  }
}

export * as TwiggModels from "./models"

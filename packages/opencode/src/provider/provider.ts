import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import fuzzysort from "fuzzysort"
import { Config } from "@/config/config"
import { mapValues, mergeDeep, omit, pickBy, sortBy } from "remeda"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Auth } from "../auth"
import { Env } from "../env"
import { Global } from "@opencode-ai/core/global"
import path from "path"
import { Effect, Layer, Context, Schema, Types } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { isRecord } from "@/util/record"
import { optional } from "@opencode-ai/core/schema"
import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ModelStatus } from "./model-status"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { TwiggClient } from "@/twigg/client"
import { TwiggModels } from "@/twigg/models"

// Twigg is the only provider (Phase 7 of the Twigg port). Its models come from the Twigg API; config can add or
// adjust models under provider.twigg, and the key comes from config, TWIGG_API_KEY or auth.json.

const ProviderApiInfo = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  npm: Schema.String,
})

const ProviderModalities = Schema.Struct({
  text: Schema.Boolean,
  audio: Schema.Boolean,
  image: Schema.Boolean,
  video: Schema.Boolean,
  pdf: Schema.Boolean,
})

const ProviderInterleavedField = Schema.Union([
  Schema.Literals(["reasoning", "reasoning_content", "reasoning_text"]),
  Schema.String,
])

const ProviderInterleaved = Schema.Union([
  Schema.Boolean,
  Schema.Struct({
    field: ProviderInterleavedField,
  }),
])

const ProviderCapabilities = Schema.Struct({
  temperature: Schema.Boolean,
  reasoning: Schema.Boolean,
  attachment: Schema.Boolean,
  toolcall: Schema.Boolean,
  input: ProviderModalities,
  output: ProviderModalities,
  interleaved: ProviderInterleaved,
})

const ProviderCacheCost = Schema.Struct({
  read: Schema.Finite,
  write: Schema.Finite,
})

const ProviderCostTier = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  cache: ProviderCacheCost,
  tier: Schema.Struct({
    type: Schema.Literal("context"),
    size: Schema.Finite,
  }),
})

const ProviderCost = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  cache: ProviderCacheCost,
  tiers: optional(Schema.Array(ProviderCostTier)),
  experimentalOver200K: optional(
    Schema.Struct({
      input: Schema.Finite,
      output: Schema.Finite,
      cache: ProviderCacheCost,
    }),
  ),
})

const ProviderLimit = Schema.Struct({
  context: Schema.Finite,
  input: optional(Schema.Finite),
  output: Schema.Finite,
})

export const Model = Schema.Struct({
  id: ModelV2.ID,
  providerID: ProviderV2.ID,
  api: ProviderApiInfo,
  name: Schema.String,
  family: optional(Schema.String),
  capabilities: ProviderCapabilities,
  cost: ProviderCost,
  limit: ProviderLimit,
  status: ModelStatus,
  options: Schema.Record(Schema.String, Schema.Any),
  headers: Schema.Record(Schema.String, Schema.String),
  release_date: Schema.String,
  variants: optional(Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Any))),
}).annotate({ identifier: "Model" })
export type Model = Types.DeepMutable<Schema.Schema.Type<typeof Model>>

export const Info = Schema.Struct({
  id: ProviderV2.ID,
  name: Schema.String,
  source: Schema.Literals(["env", "config", "custom", "api"]),
  env: Schema.Array(Schema.String),
  key: optional(Schema.String),
  options: Schema.Record(Schema.String, Schema.Any),
  models: Schema.Record(Schema.String, Model),
}).annotate({ identifier: "Provider" })
export type Info = Types.DeepMutable<Schema.Schema.Type<typeof Info>>

const DefaultModelIDs = Schema.Record(Schema.String, Schema.String)

export const ListResult = Schema.Struct({
  all: Schema.Array(Info),
  default: DefaultModelIDs,
  connected: Schema.Array(Schema.String),
})
export type ListResult = Types.DeepMutable<Schema.Schema.Type<typeof ListResult>>

export const ConfigProvidersResult = Schema.Struct({
  providers: Schema.Array(Info),
  default: DefaultModelIDs,
})
export type ConfigProvidersResult = Types.DeepMutable<Schema.Schema.Type<typeof ConfigProvidersResult>>

export function toPublicInfo(provider: Info): Info {
  return JSON.parse(
    JSON.stringify(
      {
        ...provider,
        models: Object.fromEntries(Object.entries(provider.models).filter(([, model]) => Schema.is(Model)(model))),
      },
      (_, value) => {
        if (typeof value === "function" || typeof value === "symbol" || value === undefined) return undefined
        if (typeof value === "bigint") return value.toString()
        return value
      },
    ),
  )
}

export function defaultModelIDs<T extends { models: Record<string, { id: string }> }>(providers: Record<string, T>) {
  return mapValues(
    providers,
    (item, id) =>
      (id === TwiggModels.PROVIDER_ID ? TwiggModels.preferredModel(item.models) : undefined) ??
      sort(Object.values(item.models))[0].id,
  )
}

export class ModelNotFoundError extends Schema.TaggedErrorClass<ModelNotFoundError>()("ProviderModelNotFoundError", {
  providerID: ProviderV2.ID,
  modelID: ModelV2.ID,
  suggestions: Schema.optional(Schema.Array(Schema.String)),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    const suggestions = this.suggestions?.length ? ` Did you mean: ${this.suggestions.join(", ")}?` : ""
    return `Model not found: ${this.providerID}/${this.modelID}.${suggestions}`
  }

  static isInstance(input: unknown): input is ModelNotFoundError {
    return input instanceof ModelNotFoundError
  }
}

export class InitError extends Schema.TaggedErrorClass<InitError>()("ProviderInitError", {
  providerID: ProviderV2.ID,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    return `Failed to initialize provider: ${this.providerID}`
  }

  static isInstance(input: unknown): input is InitError {
    return input instanceof InitError
  }
}

export class NoProvidersError extends Schema.TaggedErrorClass<NoProvidersError>()("ProviderNoProvidersError", {}) {
  override get message() {
    return "No providers are available"
  }

  static isInstance(input: unknown): input is NoProvidersError {
    return input instanceof NoProvidersError
  }
}

export class NoModelsError extends Schema.TaggedErrorClass<NoModelsError>()("ProviderNoModelsError", {
  providerID: ProviderV2.ID,
}) {
  override get message() {
    return `No models are available for provider: ${this.providerID}`
  }

  static isInstance(input: unknown): input is NoModelsError {
    return input instanceof NoModelsError
  }
}

export type DefaultModelError = ModelNotFoundError | NoProvidersError | NoModelsError
export type Error = ModelNotFoundError | InitError | NoProvidersError | NoModelsError

export interface Interface {
  readonly list: () => Effect.Effect<Record<ProviderV2.ID, Info>>
  readonly getProvider: (providerID: ProviderV2.ID) => Effect.Effect<Info>
  readonly getModel: (providerID: ProviderV2.ID, modelID: ModelV2.ID) => Effect.Effect<Model, ModelNotFoundError>
  readonly closest: (
    providerID: ProviderV2.ID,
    query: string[],
  ) => Effect.Effect<{ providerID: ProviderV2.ID; modelID: string } | undefined>
  readonly getSmallModel: (providerID: ProviderV2.ID) => Effect.Effect<Model | undefined>
  readonly defaultModel: () => Effect.Effect<{ providerID: ProviderV2.ID; modelID: ModelV2.ID }, DefaultModelError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Provider") {}

export const use = serviceUse(Service)

function modelSuggestions(provider: Info | undefined, modelID: ModelV2.ID, enableExperimentalModels: boolean) {
  const available = provider
    ? Object.keys(provider.models).filter((id) => {
        const model = provider.models[id]
        if (model.status === "deprecated") return false
        if (model.status === "alpha" && !enableExperimentalModels) return false
        return true
      })
    : []
  const fuzzy = fuzzysort.go(modelID, available, { limit: 3, threshold: -10000 }).map((m) => m.target)
  if (fuzzy.length) return fuzzy
  const query = modelID
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((part) => part.length > 1)
  return sortBy(
    available
      .map((id) => ({
        id,
        score: query.filter((part) => id.toLowerCase().includes(part)).length,
      }))
      .filter((item) => item.score > 0),
    [(item) => item.score, "desc"],
    [(item) => item.id, "asc"],
  )
    .slice(0, 3)
    .map((item) => item.id)
}

interface State {
  providers: Record<ProviderV2.ID, Info>
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const config = yield* Config.Service
    const auth = yield* Auth.Service
    const env = yield* Env.Service
    const runtimeFlags = yield* RuntimeFlags.Service
    const twiggModels = yield* TwiggModels.Service

    const state = yield* InstanceState.make<State>(() =>
      Effect.gen(function* () {
        const cfg = yield* config.get()
        const id = TwiggModels.PROVIDER_ID
        const allowed = (cfg.enabled_providers?.includes(id) ?? true) && !cfg.disabled_providers?.includes(id)
        if (!allowed) return { providers: {} }

        const configured = cfg.provider?.[id]
        const baseURL = cfg.twigg?.baseURL ?? TwiggClient.DEFAULT_BASE_URL
        const stored = yield* auth.get(id).pipe(Effect.orDie)
        const fromConfig = typeof configured?.options?.apiKey === "string" ? configured.options.apiKey : undefined
        const fromEnv = yield* env.get(TwiggModels.ENV_KEY)
        const apiKey = fromConfig ?? fromEnv ?? (stored?.type === "api" ? stored.key : undefined)
        const catalogue = TwiggModels.toProvider(apiKey ? yield* twiggModels.get({ baseURL, apiKey }) : [], baseURL)
        const provider: Info = {
          ...catalogue,
          name: configured?.name ?? catalogue.name,
          source: fromConfig || configured ? "config" : fromEnv ? "env" : "api",
          key: fromEnv ?? (stored?.type === "api" ? stored.key : undefined),
          options: mergeDeep(catalogue.options, configured?.options ?? {}),
          models: {
            ...catalogue.models,
            ...mapValues(configured?.models ?? {}, (model, modelID) =>
              configModel(model, modelID, catalogue.models[model.id ?? modelID], baseURL),
            ),
          },
        }
        if (!apiKey && !configured) return { providers: {} }

        for (const [modelID, model] of Object.entries(provider.models)) {
          if (model.status === "alpha" && !runtimeFlags.enableExperimentalModels) delete provider.models[modelID]
          if (model.status === "deprecated") delete provider.models[modelID]
          if (
            (configured?.blacklist && configured.blacklist.includes(modelID)) ||
            (configured?.whitelist && !configured.whitelist.includes(modelID))
          )
            delete provider.models[modelID]
        }
        if (Object.keys(provider.models).length === 0) return { providers: {} }
        return { providers: { [id]: provider } as Record<ProviderV2.ID, Info> }
      }),
    )

    const list = Effect.fn("Provider.list")(() => InstanceState.use(state, (s) => s.providers))

    const getProvider = Effect.fn("Provider.getProvider")((providerID: ProviderV2.ID) =>
      InstanceState.use(state, (s) => s.providers[providerID]),
    )

    const getModel = Effect.fn("Provider.getModel")(function* (providerID: ProviderV2.ID, modelID: ModelV2.ID) {
      const s = yield* InstanceState.get(state)
      const provider = s.providers[providerID]
      if (!provider) {
        const suggestions = fuzzysort
          .go(providerID, Object.keys(s.providers), { limit: 3, threshold: -10000 })
          .map((m) => m.target)
        return yield* new ModelNotFoundError({ providerID, modelID, suggestions })
      }
      const info = provider.models[modelID]
      if (!info) {
        const suggestions = modelSuggestions(provider, modelID, runtimeFlags.enableExperimentalModels)
        return yield* new ModelNotFoundError({ providerID, modelID, suggestions })
      }
      return info
    })

    const closest = Effect.fn("Provider.closest")(function* (providerID: ProviderV2.ID, query: string[]) {
      const s = yield* InstanceState.get(state)
      const provider = s.providers[providerID]
      if (!provider) return undefined
      for (const item of query) {
        for (const modelID of Object.keys(provider.models)) {
          if (modelID.includes(item)) return { providerID, modelID }
        }
      }
      return undefined
    })

    // Side calls (titles, names) use this. Twigg can't run them until it has throwaway calls (api-requests.md #5),
    // so it only matters when small_model is configured.
    const getSmallModel = Effect.fn("Provider.getSmallModel")(function* (_providerID: ProviderV2.ID) {
      const cfg = yield* config.get()
      if (!cfg.small_model) return undefined
      const parsed = parseModel(cfg.small_model)
      return yield* getModel(parsed.providerID, parsed.modelID).pipe(
        Effect.catchTag("ProviderModelNotFoundError", () => Effect.succeed(undefined)),
      )
    })

    const defaultModel = Effect.fn("Provider.defaultModel")(function* () {
      const cfg = yield* config.get()
      if (cfg.model) return parseModel(cfg.model)

      const s = yield* InstanceState.get(state)
      const recent = yield* fs.readJson(path.join(Global.Path.state, "model.json")).pipe(
        Effect.map((x): { providerID: ProviderV2.ID; modelID: ModelV2.ID }[] => {
          if (!isRecord(x) || !Array.isArray(x.recent)) return []
          return x.recent.flatMap((item) => {
            if (!isRecord(item)) return []
            if (typeof item.providerID !== "string") return []
            if (typeof item.modelID !== "string") return []
            return [{ providerID: ProviderV2.ID.make(item.providerID), modelID: ModelV2.ID.make(item.modelID) }]
          })
        }),
        Effect.catch(() => Effect.succeed([] as { providerID: ProviderV2.ID; modelID: ModelV2.ID }[])),
      )
      for (const entry of recent) {
        const provider = s.providers[entry.providerID]
        if (!provider) continue
        if (!provider.models[entry.modelID]) continue
        return { providerID: entry.providerID, modelID: entry.modelID }
      }

      const twigg = s.providers[TwiggModels.PROVIDER_ID]
      if (!twigg) return yield* new NoProvidersError()
      if (Object.keys(twigg.models).length === 0) return yield* new NoModelsError({ providerID: twigg.id })
      return { providerID: twigg.id, modelID: ModelV2.ID.make(defaultModelIDs({ twigg }).twigg) }
    })

    return Service.of({ list, getProvider, getModel, closest, getSmallModel, defaultModel })
  }),
)

type ConfigModel = NonNullable<NonNullable<ConfigV1.Info["provider"]>[string]["models"]>[string]

// A model declared or adjusted under provider.twigg.models in config. It always runs through the Twigg runtime.
function configModel(model: ConfigModel, modelID: string, existing: Model | undefined, baseURL: string): Model {
  const parsed: Model = {
    id: ModelV2.ID.make(modelID),
    api: { id: model.id ?? existing?.api.id ?? modelID, npm: TwiggModels.NPM, url: existing?.api.url ?? baseURL },
    status: model.status ?? existing?.status ?? "active",
    name: model.name ?? existing?.name ?? modelID,
    providerID: TwiggModels.PROVIDER_ID,
    capabilities: {
      temperature: false,
      reasoning: model.reasoning ?? existing?.capabilities.reasoning ?? false,
      attachment: model.attachment ?? existing?.capabilities.attachment ?? false,
      toolcall: model.tool_call ?? existing?.capabilities.toolcall ?? true,
      input: {
        text: model.modalities?.input?.includes("text") ?? existing?.capabilities.input.text ?? true,
        audio: model.modalities?.input?.includes("audio") ?? existing?.capabilities.input.audio ?? false,
        image: model.modalities?.input?.includes("image") ?? existing?.capabilities.input.image ?? false,
        video: model.modalities?.input?.includes("video") ?? existing?.capabilities.input.video ?? false,
        pdf: model.modalities?.input?.includes("pdf") ?? existing?.capabilities.input.pdf ?? false,
      },
      output: { text: true, audio: false, image: false, video: false, pdf: false },
      interleaved: false,
    },
    cost: {
      input: model.cost?.input ?? existing?.cost.input ?? 0,
      output: model.cost?.output ?? existing?.cost.output ?? 0,
      cache: {
        read: model.cost?.cache_read ?? existing?.cost.cache.read ?? 0,
        write: model.cost?.cache_write ?? existing?.cost.cache.write ?? 0,
      },
      tiers: existing?.cost.tiers,
    },
    options: mergeDeep(existing?.options ?? {}, model.options ?? {}),
    limit: {
      context: model.limit?.context ?? existing?.limit.context ?? 0,
      input: model.limit?.input ?? existing?.limit.input,
      output: model.limit?.output ?? existing?.limit.output ?? 0,
    },
    headers: mergeDeep(existing?.headers ?? {}, model.headers ?? {}),
    family: model.family ?? existing?.family ?? "",
    release_date: model.release_date ?? existing?.release_date ?? "",
    variants: {},
  }
  const merged = mergeDeep(existing?.variants ?? {}, model.variants ?? {})
  parsed.variants = mapValues(
    pickBy(merged, (v) => !v.disabled),
    (v) => omit(v, ["disabled"]),
  )
  return parsed
}

const priority = ["gpt-5", "claude-sonnet-4", "big-pickle", "gemini-3-pro"]
const smallModelFamilyPriority = ["gemini-flash", "gpt-nano", "claude-haiku"]
export function sort<T extends { id: string }>(models: T[]) {
  return sortBy(
    models,
    [(model) => priority.findIndex((filter) => model.id.includes(filter)), "desc"],
    [(model) => (model.id.includes("latest") ? 0 : 1), "asc"],
    [(model) => model.id, "desc"],
  )
}

export function parseModel(model: string) {
  const [providerID, ...rest] = model.split("/")
  return {
    providerID: ProviderV2.ID.make(providerID),
    modelID: ModelV2.ID.make(rest.join("/")),
  }
}

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [FSUtil.node, Config.node, Auth.node, Env.node, RuntimeFlags.node, TwiggModels.node],
})

export * as Provider from "./provider"

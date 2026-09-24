import { afterEach, expect, test } from "bun:test"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ModelV2 } from "@opencode-ai/core/model"
import { Cause, Effect, Exit, Layer } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Env } from "@/env"
import { Provider } from "@/provider/provider"
import { TwiggModels } from "@/twigg/models"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { catalogueModel } from "./fixture"

const requests: { url: string; authorization: string | undefined }[] = []
const original = process.env[TwiggModels.ENV_KEY]

afterEach(async () => {
  if (original === undefined) delete process.env[TwiggModels.ENV_KEY]
  if (original !== undefined) process.env[TwiggModels.ENV_KEY] = original
  await disposeAllInstances()
})

const it = testEffect(
  LayerNode.compile(
    LayerNode.group([
      Provider.node,
      FSUtil.node,
      Env.node,
      Config.node,
      Auth.node,
      RuntimeFlags.node,
      TwiggModels.node,
    ]),
    [
      [
        httpClient,
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) => {
            requests.push({ url: request.url, authorization: request.headers.authorization })
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                request.url.endsWith("/models")
                  ? Response.json([
                      catalogueModel({ name: "gpt-6-luna", max_reasoning_effort: "high" }),
                      catalogueModel({ name: "claude-sonnet-5" }),
                    ])
                  : new Response("not found", { status: 404 }),
              ),
            )
          }),
        ),
      ],
    ],
  ),
)

const withKey = (key: string) => Effect.sync(() => (process.env[TwiggModels.ENV_KEY] = key))
const withoutKey = Effect.sync(() => delete process.env[TwiggModels.ENV_KEY])

it.instance(
  "a TWIGG_API_KEY connects twigg with its models",
  Effect.gen(function* () {
    yield* withKey("tw_env_key")
    const providers = yield* Provider.use.list()
    const twigg = providers[TwiggModels.PROVIDER_ID]
    expect(twigg.source).toBe("env")
    expect(Object.keys(twigg.models).sort()).toEqual(["claude-sonnet-5", "gpt-6-luna"])
    expect(Object.keys(twigg.models["gpt-6-luna"].variants ?? {})).toEqual(["off", "low", "medium", "high"])
    const request = requests.find((item) => item.url === "https://env.twigg.test/api/v1/models")
    expect(request?.authorization).toBe("Bearer tw_env_key")
  }),
  { config: { twigg: { baseURL: "https://env.twigg.test/api/v1" } } },
)

it.instance(
  "a key in auth.json connects twigg",
  Effect.gen(function* () {
    yield* withoutKey
    const auth = yield* Auth.Service
    yield* auth.set(TwiggModels.PROVIDER_ID, { type: "api", key: "tw_stored_key" })
    const providers = yield* Provider.use.list()
    yield* auth.remove(TwiggModels.PROVIDER_ID)
    expect(providers[TwiggModels.PROVIDER_ID].source).toBe("api")
    const request = requests.find((item) => item.url === "https://auth.twigg.test/api/v1/models")
    expect(request?.authorization).toBe("Bearer tw_stored_key")
  }),
  { config: { twigg: { baseURL: "https://auth.twigg.test/api/v1" } } },
)

it.instance(
  "without a key twigg is not connected and nothing is fetched",
  Effect.gen(function* () {
    yield* withoutKey
    const providers = yield* Provider.use.list()
    expect(providers[TwiggModels.PROVIDER_ID]).toBeUndefined()
    expect(requests.some((item) => item.url.startsWith("https://nokey.twigg.test"))).toBe(false)
  }),
  { config: { twigg: { baseURL: "https://nokey.twigg.test/api/v1" } } },
)

it.instance(
  "disabled_providers skips twigg entirely",
  Effect.gen(function* () {
    yield* withKey("tw_env_key")
    const providers = yield* Provider.use.list()
    expect(providers[TwiggModels.PROVIDER_ID]).toBeUndefined()
    expect(requests.some((item) => item.url.startsWith("https://disabled.twigg.test"))).toBe(false)
  }),
  { config: { disabled_providers: ["twigg"], twigg: { baseURL: "https://disabled.twigg.test/api/v1" } } },
)

it.instance(
  "enabled_providers without twigg skips it entirely",
  Effect.gen(function* () {
    yield* withKey("tw_env_key")
    const providers = yield* Provider.use.list()
    expect(providers[TwiggModels.PROVIDER_ID]).toBeUndefined()
    expect(requests.some((item) => item.url.startsWith("https://enabled.twigg.test"))).toBe(false)
  }),
  { config: { enabled_providers: ["anthropic"], twigg: { baseURL: "https://enabled.twigg.test/api/v1" } } },
)

it.instance(
  "twigg is the default model when nothing else is chosen",
  Effect.gen(function* () {
    yield* withKey("tw_env_key")
    const model = yield* Provider.use.defaultModel()
    expect(model).toEqual({ providerID: TwiggModels.PROVIDER_ID, modelID: ModelV2.ID.make("claude-sonnet-5") })
  }),
  { config: { twigg: { baseURL: "https://default.twigg.test/api/v1" } } },
)

it.instance(
  "config can supply the key and add or adjust models",
  Effect.gen(function* () {
    yield* withoutKey
    const twigg = (yield* Provider.use.list())[TwiggModels.PROVIDER_ID]
    expect(twigg.source).toBe("config")
    expect(TwiggModels.settings(twigg)?.apiKey).toBe("tw_config_key")
    expect(twigg.models["my-alias"]).toMatchObject({
      api: { id: "gpt-6-luna", npm: TwiggModels.NPM },
      name: "Luna alias",
      limit: { context: 1_000_000, output: 128_000 },
    })
    expect(twigg.models["claude-sonnet-5"].limit.output).toBe(4096)
  }),
  {
    config: {
      twigg: { baseURL: "https://config.twigg.test/api/v1" },
      provider: {
        twigg: {
          options: { apiKey: "tw_config_key" },
          models: {
            "my-alias": { id: "gpt-6-luna", name: "Luna alias" },
            "claude-sonnet-5": { limit: { context: 1_000_000, output: 4096 } },
          },
        },
      },
    },
  },
)

it.instance(
  "whitelist and blacklist narrow the models",
  Effect.gen(function* () {
    yield* withKey("tw_env_key")
    const twigg = (yield* Provider.use.list())[TwiggModels.PROVIDER_ID]
    expect(Object.keys(twigg.models)).toEqual(["gpt-6-luna"])
  }),
  {
    config: {
      twigg: { baseURL: "https://lists.twigg.test/api/v1" },
      provider: { twigg: { whitelist: ["gpt-6-luna", "claude-sonnet-5"], blacklist: ["claude-sonnet-5"] } },
    },
  },
)

test("the list endpoint's default map prefers the Twigg default model", () => {
  const twigg = TwiggModels.toProvider(
    [catalogueModel({ name: "openrouter/gpt-5-6-sol" }), catalogueModel({ name: "claude-sonnet-5" })],
    "https://twigg.test/api/v1",
  )
  expect(Provider.defaultModelIDs({ twigg }).twigg).toBe(ModelV2.ID.make("claude-sonnet-5"))
})

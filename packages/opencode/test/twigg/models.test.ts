import { describe, expect, test } from "bun:test"
import { utimes } from "fs/promises"
import path from "path"
import { Global } from "@opencode-ai/core/global"
import { ModelV2 } from "@opencode-ai/core/model"
import { Hash } from "@opencode-ai/core/util/hash"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect, Layer } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { TwiggModels } from "../../src/twigg/models"
import { testEffect } from "../lib/effect"
import { catalogueModel, slot } from "./fixture"

describe("twigg model mapping", () => {
  const baseURL = "https://twigg.test/api/v1"

  test("maps limits, prices, capabilities and api", () => {
    const model = TwiggModels.toModel(catalogueModel({ name: "fireworks/glm-5.2" }), baseURL)
    expect(model.id).toBe(ModelV2.ID.make("fireworks/glm-5.2"))
    expect(model.providerID).toBe(TwiggModels.PROVIDER_ID)
    expect(model.name).toBe("FIREWORKS/GLM-5.2")
    expect(model.api).toEqual({ id: "fireworks/glm-5.2", url: baseURL, npm: TwiggModels.NPM })
    expect(model.limit).toEqual({ context: 1_000_000, output: 128_000 })
    expect(model.cost).toEqual({ input: 10.5, output: 52.5, cache: { read: 0.2625, write: 13.125 }, tiers: [] })
    expect(model.capabilities.attachment).toBe(true)
    expect(model.capabilities.input).toEqual({ text: true, audio: false, image: true, video: false, pdf: true })
    expect(model.capabilities.temperature).toBe(false)
  })

  test("maps long-context cliff tiers so they apply above min - 1", () => {
    const model = TwiggModels.toModel(
      catalogueModel({
        name: "gpt-6-luna",
        tiers: [
          {
            mode: "cliff",
            metric: "contexttokens",
            min: 272001,
            input: "0.21",
            output: "0.7875",
            cache_read: "0.021",
            cache_write: "0.2625",
          },
          {
            mode: "other",
            metric: "contexttokens",
            min: 1,
            input: "1",
            output: "1",
            cache_read: "1",
            cache_write: "1",
          },
        ],
      }),
      baseURL,
    )
    expect(model.cost.tiers).toEqual([
      { input: 0.21, output: 0.7875, cache: { read: 0.021, write: 0.2625 }, tier: { type: "context", size: 272000 } },
    ])
  })

  test("variants go up to max_reasoning_effort", () => {
    const model = TwiggModels.toModel(catalogueModel({ name: "a", max_reasoning_effort: "high" }), baseURL)
    expect(Object.keys(model.variants ?? {})).toEqual(["off", "low", "medium", "high"])
    expect(model.variants?.high).toEqual({ reasoning_effort: "high" })
  })

  test("a null max_reasoning_effort offers every effort", () => {
    const model = TwiggModels.toModel(catalogueModel({ name: "a" }), baseURL)
    expect(Object.keys(model.variants ?? {})).toEqual([...TwiggModels.EFFORTS])
  })

  test("models without reasoning get no variants", () => {
    const model = TwiggModels.toModel(catalogueModel({ name: "a", supports_reasoning: false }), baseURL)
    expect(model.variants).toEqual({})
    expect(model.capabilities.reasoning).toBe(false)
  })

  test("models without user media get no attachments", () => {
    const model = TwiggModels.toModel(
      catalogueModel({ name: "a", media: { user: slot([]), tool_result: slot([]) } }),
      baseURL,
    )
    expect(model.capabilities.attachment).toBe(false)
  })
})

describe("twigg model loading", () => {
  const calls: string[] = []
  const state = { fail: false }
  const layer = LayerNode.compile(TwiggModels.node, [
    [
      httpClient,
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request: HttpClientRequest.HttpClientRequest) => {
          calls.push(request.url)
          const response = state.fail
            ? Response.json({ error: { code: "unavailable", message: "down" } }, { status: 503 })
            : Response.json([
                catalogueModel({ name: "gpt-6-luna" }),
                // Live data has null cache prices; a malformed entry must not hide the others.
                catalogueModel({
                  name: "gpt-5-6-sol",
                  rates: { input: "1", output: "2", cache_read: "0.1", cache_write: null },
                }),
                { name: "broken" },
              ])
          return Effect.succeed(HttpClientResponse.fromWeb(request, response))
        }),
      ),
    ],
  ])
  const it = testEffect(layer)

  it.effect("fetches, then serves the fresh cache without refetching", () =>
    Effect.gen(function* () {
      const svc = yield* TwiggModels.Service
      const settings = { baseURL: "https://fresh.twigg.test/api/v1", apiKey: "k" }
      const first = yield* svc.get(settings)
      const second = yield* svc.get(settings)
      expect(first.map((model) => model.name)).toEqual(["gpt-6-luna", "gpt-5-6-sol"])
      expect(second).toEqual(first)
      expect(calls.filter((url) => url.startsWith(settings.baseURL))).toEqual([`${settings.baseURL}/models`])
    }),
  )

  it.effect("falls back to a stale cache when the API fails", () =>
    Effect.gen(function* () {
      const svc = yield* TwiggModels.Service
      const settings = { baseURL: "https://stale.twigg.test/api/v1", apiKey: "k" }
      yield* svc.get(settings)
      const file = path.join(Global.Path.cache, `twigg-models-${Hash.fast(settings.baseURL).slice(0, 8)}.json`)
      yield* Effect.promise(() => utimes(file, new Date(0), new Date(0)))
      state.fail = true
      const models = yield* svc.get(settings)
      state.fail = false
      expect(models.map((model) => model.name)).toEqual(["gpt-6-luna", "gpt-5-6-sol"])
      expect(calls.filter((url) => url.startsWith(settings.baseURL))).toHaveLength(2)
    }),
  )

  it.effect("returns no models when the API fails and nothing is cached", () =>
    Effect.gen(function* () {
      state.fail = true
      const svc = yield* TwiggModels.Service
      const models = yield* svc.get({ baseURL: "https://down.twigg.test/api/v1", apiKey: "k" })
      state.fail = false
      expect(models).toEqual([])
    }),
  )
})

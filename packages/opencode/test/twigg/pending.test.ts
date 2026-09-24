import { describe, expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import { HttpClient } from "effect/unstable/http"
import { TwiggPending } from "../../src/twigg/pending"
import { it } from "../lib/effect"

// The stubs never call it, but they already require it like the real calls will.
const http = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make(() => Effect.die("stubs must not make requests")),
)

const settings = { baseURL: "https://twigg.test/api/v1", apiKey: "tw_test_key" }

describe("twigg pending endpoints", () => {
  it.effect("every stub fails with TwiggApiPending and its issue number", () =>
    Effect.gen(function* () {
      const errors = yield* Effect.all([
        TwiggPending.cancelRun(settings, "r1").pipe(Effect.flip),
        TwiggPending.forkChat(settings, "c1", 3).pipe(Effect.flip),
        TwiggPending.updateChat(settings, "c1", { title: "x" }).pipe(Effect.flip),
        TwiggPending.respondOnce(settings, { model: "gpt-6-luna", input: [] }).pipe(Stream.runCollect, Effect.flip),
      ])
      errors.forEach((error) => expect(error).toBeInstanceOf(TwiggPending.TwiggApiPending))
      expect(errors.map((error) => error._tag === "TwiggApiPending" && error.issue)).toEqual([1, 2, 4, 5])
    }).pipe(Effect.provide(http)),
  )
})

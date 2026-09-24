import { Effect, Schema, Stream } from "effect"
import type { HttpClient } from "effect/unstable/http"
import type { TwiggClient } from "./client"

// Twigg endpoints the port needs but that don't exist yet (context/twigg/api-requests.md). Each stub has the signature
// the real call will have and only fails with TwiggApiPending. Call sites handle that error with interim behaviour,
// marked with the same TODO(twigg-api#N). Find them all with: grep -rn "TODO(twigg-api#" packages/

export class TwiggApiPending extends Schema.TaggedErrorClass<TwiggApiPending>()("TwiggApiPending", {
  endpoint: Schema.String,
  issue: Schema.Number,
}) {}

type Failure = TwiggApiPending | TwiggClient.ApiError | TwiggClient.TransportError

export interface ChatPatch {
  readonly title?: string
  readonly description?: string | null
  readonly user_metadata?: Schema.Json
}

export interface RespondOnceRequest {
  readonly model: string
  readonly input: ReadonlyArray<Schema.Json>
  readonly tools?: ReadonlyArray<Schema.Json>
  readonly max_tokens?: number
  readonly reasoning_effort?: string
  readonly namespace?: string
  readonly instructions?: string
}

export function cancelRun(
  _settings: TwiggClient.Settings,
  _runID: string,
): Effect.Effect<void, Failure, HttpClient.HttpClient> {
  // TODO(twigg-api#1): implement once POST /runs/{id}/cancel ships
  return Effect.fail(new TwiggApiPending({ endpoint: "POST /runs/{id}/cancel", issue: 1 }))
}

export function forkChat(
  _settings: TwiggClient.Settings,
  _chatID: string,
  _ordinal: number,
): Effect.Effect<{ readonly id: string }, Failure, HttpClient.HttpClient> {
  // TODO(twigg-api#2): implement once POST /chats/{id}/fork ships
  return Effect.fail(new TwiggApiPending({ endpoint: "POST /chats/{id}/fork", issue: 2 }))
}

export function updateChat(
  _settings: TwiggClient.Settings,
  _chatID: string,
  _patch: ChatPatch,
): Effect.Effect<void, Failure, HttpClient.HttpClient> {
  // TODO(twigg-api#4): implement once PATCH /chats/{id} ships
  return Effect.fail(new TwiggApiPending({ endpoint: "PATCH /chats/{id}", issue: 4 }))
}

export function respondOnce(
  _settings: TwiggClient.Settings,
  _request: RespondOnceRequest,
): Stream.Stream<TwiggClient.Event, TwiggApiPending | TwiggClient.Error, HttpClient.HttpClient> {
  // TODO(twigg-api#5): implement once POST /responses ships
  return Stream.fail(new TwiggApiPending({ endpoint: "POST /responses", issue: 5 }))
}

export * as TwiggPending from "./pending"

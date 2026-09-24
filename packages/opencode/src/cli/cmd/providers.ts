import type { Argv } from "yargs"
import { Auth } from "../../auth"
import { cmd } from "./cmd"
import { CliError, effectCmd, fail } from "../effect-cmd"
import { UI } from "../ui"
import * as Prompt from "../effect/prompt"
import { TwiggModels } from "@/twigg/models"
import { TwiggClient } from "@/twigg/client"
import { FetchHttpClient } from "effect/unstable/http"

import path from "path"
import os from "os"
import { Config } from "@/config/config"
import { Global } from "@opencode-ai/core/global"
import { Process } from "@/util/process"
import { errorMessage } from "@/util/error"
import { text } from "node:stream/consumers"
import { Effect, Option, Result, Schema } from "effect"

const promptValue = <Value>(value: Option.Option<Value>) => {
  if (Option.isNone(value)) return Effect.die(new UI.CancelledError())
  return Effect.succeed(value.value)
}

const cliTry = <Value>(message: string, fn: () => PromiseLike<Value>) =>
  Effect.tryPromise({
    try: fn,
    catch: (error) => new CliError({ message: message + errorMessage(error) }),
  })

export const ProvidersCommand = cmd({
  command: "providers",
  aliases: ["auth"],
  describe: "manage AI providers and credentials",
  builder: (yargs) =>
    yargs.command(ProvidersListCommand).command(ProvidersLoginCommand).command(ProvidersLogoutCommand).demandCommand(),
  async handler() {},
})

export const ProvidersListCommand = effectCmd({
  command: "list",
  aliases: ["ls"],
  describe: "list providers and credentials",
  // Lists global credentials + provider env vars; no project instance needed.
  instance: false,
  handler: Effect.fn("Cli.providers.list")(function* (_args) {
    const authSvc = yield* Auth.Service

    UI.empty()
    const authPath = path.join(Global.Path.data, "auth.json")
    const homedir = os.homedir()
    const displayPath = authPath.startsWith(homedir) ? authPath.replace(homedir, "~") : authPath
    yield* Prompt.intro(`Credentials ${UI.Style.TEXT_DIM}${displayPath}`)
    const results = Object.entries(yield* Effect.orDie(authSvc.all()))

    for (const [providerID, result] of results) {
      const name = providerID === TwiggModels.PROVIDER_ID ? "Twigg" : providerID
      yield* Prompt.log.info(`${name} ${UI.Style.TEXT_DIM}${result.type}`)
    }

    yield* Prompt.outro(`${results.length} credentials`)

    const activeEnvVars: Array<{ provider: string; envVar: string }> = process.env[TwiggModels.ENV_KEY]
      ? [{ provider: "Twigg", envVar: TwiggModels.ENV_KEY }]
      : []

    if (activeEnvVars.length > 0) {
      UI.empty()
      yield* Prompt.intro("Environment")

      for (const { provider, envVar } of activeEnvVars) {
        yield* Prompt.log.info(`${provider} ${UI.Style.TEXT_DIM}${envVar}`)
      }

      yield* Prompt.outro(`${activeEnvVars.length} environment variable` + (activeEnvVars.length === 1 ? "" : "s"))
    }
  }),
})

export const ProvidersLoginCommand = effectCmd({
  command: "login [url]",
  describe: "log in to a provider",
  // URL login skips instance bootstrap, which would load remote config with the stale token and crash before re-auth.
  instance: (args) => !args.url,
  builder: (yargs: Argv) =>
    yargs
      .positional("url", {
        describe: "opencode auth provider",
        type: "string",
      })
      .option("provider", {
        alias: ["p"],
        describe: "provider id or name to log in to (skips provider selection)",
        type: "string",
      })
      .option("method", {
        alias: ["m"],
        describe: "login method label (skips method selection)",
        type: "string",
      }),
  handler: Effect.fn("Cli.providers.login")(function* (args) {
    const authSvc = yield* Auth.Service

    UI.empty()
    yield* Prompt.intro("Add credential")
    if (args.url) {
      const url = args.url.replace(/\/+$/, "")
      const wellknown = (yield* cliTry(`Failed to load auth provider metadata from ${url}: `, () =>
        fetch(`${url}/.well-known/opencode`).then((x) => x.json()),
      )) as {
        auth: { command: string[]; env: string }
      }
      yield* Prompt.log.info(`Running \`${wellknown.auth.command.join(" ")}\``)
      const abort = new AbortController()
      const proc = Process.spawn(wellknown.auth.command, { stdout: "pipe", stderr: "inherit", abort: abort.signal })
      if (!proc.stdout) {
        yield* Prompt.log.error("Failed")
        yield* Prompt.outro("Done")
        return
      }
      const [exit, token] = yield* cliTry("Failed to run auth provider command: ", () =>
        Promise.all([proc.exited, text(proc.stdout!)]),
      ).pipe(Effect.ensuring(Effect.sync(() => abort.abort())))
      if (exit !== 0) {
        yield* Prompt.log.error("Failed")
        yield* Prompt.outro("Done")
        return
      }
      yield* Effect.orDie(authSvc.set(url, { type: "wellknown", key: wellknown.auth.env, token: token.trim() }))
      yield* Prompt.log.success("Logged into " + url)
      yield* Prompt.outro("Done")
      return
    }

    // Twigg is the only provider.
    const cfgSvc = yield* Config.Service
    const config = yield* cfgSvc.get()
    const allowed =
      (config.enabled_providers?.includes(TwiggModels.PROVIDER_ID) ?? true) &&
      !config.disabled_providers?.includes(TwiggModels.PROVIDER_ID)
    if (!allowed) return yield* fail("Twigg is turned off by enabled_providers or disabled_providers in your config")
    if (args.provider && !["twigg"].includes(args.provider.toLowerCase()))
      return yield* fail(`Unknown provider "${args.provider}". Twigg is the only provider.`)

    yield* Prompt.log.info("Create an api key at https://twigg.ai/dashboard/api-keys")
    const key = yield* Prompt.password({
      message: "Enter your Twigg API key",
      validate: (x) => (x && x.length > 0 ? undefined : "Required"),
    })
    const apiKey = yield* promptValue(key)
    if (!(yield* checkTwiggKey(apiKey))) return
    yield* Effect.orDie(authSvc.set(TwiggModels.PROVIDER_ID, { type: "api", key: apiKey }))

    yield* Prompt.outro("Done")
  }),
})

// A rejected key isn't saved. If Twigg can't be reached, the key is saved anyway and checked on first use.
const checkTwiggKey = Effect.fnUntraced(function* (apiKey: string) {
  const result = yield* TwiggClient.request(
    { baseURL: TwiggClient.DEFAULT_BASE_URL, apiKey },
    Schema.Array(Schema.Unknown),
    {
      method: "GET",
      path: "/models",
    },
  ).pipe(Effect.provide(FetchHttpClient.layer), Effect.result)
  if (Result.isSuccess(result)) {
    yield* Prompt.log.success(`Key works: ${result.success.length} Twigg models available`)
    return true
  }
  if (result.failure._tag === "TwiggRequestError" && result.failure.status === 401) {
    yield* Prompt.log.error("Twigg rejected this key. Check it at https://twigg.ai/dashboard/api-keys")
    yield* Prompt.outro("Key not saved")
    return false
  }
  yield* Prompt.log.warn(`Couldn't check the key with Twigg (${result.failure.message}). Saving it anyway.`)
  return true
})

export const ProvidersLogoutCommand = effectCmd({
  command: "logout [provider]",
  describe: "log out from a configured provider",
  builder: (yargs) =>
    yargs.positional("provider", {
      describe: "provider id or name to log out from",
      type: "string",
    }),
  // Removes a global auth credential; no project instance needed.
  instance: false,
  handler: Effect.fn("Cli.providers.logout")(function* (args) {
    const authSvc = yield* Auth.Service

    UI.empty()
    const credentials: Array<[string, Auth.Info]> = Object.entries(yield* Effect.orDie(authSvc.all()))
    yield* Prompt.intro("Remove credential")
    if (credentials.length === 0) {
      yield* Prompt.log.error("No credentials found")
      return
    }
    const name = (key: string) => (key === TwiggModels.PROVIDER_ID ? "Twigg" : key)
    const options = credentials.map(([key, value]) => ({
      label: name(key) + UI.Style.TEXT_DIM + " (" + value.type + ")",
      value: key,
    }))
    const provider = args.provider
      ? options.find(
          (option) =>
            option.value === args.provider || name(option.value).toLowerCase() === args.provider?.toLowerCase(),
        )?.value
      : yield* promptValue(
          yield* Prompt.autocomplete({
            message: "Select provider",
            maxItems: 8,
            options,
          }),
        )
    if (!provider) return yield* fail(`Unknown configured provider "${args.provider}"`)
    yield* Effect.orDie(authSvc.remove(provider))
    yield* Prompt.outro("Logout successful")
  }),
})

import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { Hash } from "@opencode-ai/core/util/hash"
import { Effect } from "effect"
import path from "path"

const SEGMENT = /^[a-z0-9_-]+$/
const MAX_SEGMENT_BYTES = 64
const MAX_BYTES = 255

// Interim root (user, 2026-09-24). The profile segment is this machine's install ID until profiles and device IDs are
// defined, so every namespace currently reads twigg-code/<install-id>/...
export const ROOT = "twigg-code"

// Twigg namespaces are immutable per chat and a typo silently falls back to the org defaults, so every namespace we
// build must already be valid rather than relying on the server to reject it.
export function namespaceFor(input: { projectID: string; directory: string; profile: string; device: string }) {
  if (input.projectID !== "global") return [ROOT, segment(input.profile), "p", segment(input.projectID)].join("/")
  return [ROOT, segment(input.profile), "d", segment(input.device), directorySegment(input.directory)].join("/")
}

// A random UUID, created on first use and kept in the state directory. It must stay stable: every chat and published
// instruction lives under it.
export const installID = Effect.fn("TwiggNamespace.installID")(function* () {
  const fs = yield* FSUtil.Service
  const file = path.join(Global.Path.state, "twigg-install-id")
  const existing = (yield* fs.readFileStringSafe(file).pipe(Effect.orDie))?.trim()
  if (existing && SEGMENT.test(existing)) return existing
  const id = crypto.randomUUID()
  yield* fs.writeWithDirs(file, id).pipe(Effect.orDie)
  return id
})

// Lowercases and replaces anything outside [a-z0-9_-] with "-", so the result is always a valid segment.
export function segment(input: string) {
  const slug = input
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, MAX_SEGMENT_BYTES)
  return slug || "none"
}

export function validate(namespace: string) {
  if (Buffer.byteLength(namespace) > MAX_BYTES) return `namespace is longer than ${MAX_BYTES} bytes`
  const invalid = namespace.split("/").find((part) => !SEGMENT.test(part) || part.length > MAX_SEGMENT_BYTES)
  if (invalid === undefined) return
  if (invalid === "") return "namespace has an empty segment"
  if (invalid.length > MAX_SEGMENT_BYTES) return `segment "${invalid}" is longer than ${MAX_SEGMENT_BYTES} bytes`
  return `segment "${invalid}" may only contain a-z, 0-9, _ and -`
}

// One segment per directory, so a parent folder's namespace subtree never includes a child folder's chats. The hash
// keeps paths that sanitise to the same slug (e.g. "/a/b-c" and "/a-b/c") apart.
function directorySegment(directory: string) {
  return `${segment(directory).slice(0, MAX_SEGMENT_BYTES - 9)}-${Hash.fast(directory).slice(0, 8)}`
}

export * as TwiggNamespace from "./namespace"

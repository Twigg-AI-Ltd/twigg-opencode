import { describe, expect, test } from "bun:test"
import { TwiggNamespace } from "../../src/twigg/namespace"

describe("twigg namespace", () => {
  test("git project uses the project id", () => {
    expect(
      TwiggNamespace.namespaceFor({
        projectID: "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
        directory: "/home/matti/code/app",
        profile: "matti",
        device: "laptop",
      }),
    ).toBe("oc/matti/p/4b825dc642cb6eb9a060e54bf8d69288fbee4904")
  })

  test("folder without git uses device and one directory segment", () => {
    const namespace = TwiggNamespace.namespaceFor({
      projectID: "global",
      directory: "/home/Matti/My Notes",
      profile: "matti",
      device: "Matti's MacBook.local",
    })
    expect(namespace).toMatch(/^oc\/matti\/d\/matti-s-macbook-local\/home-matti-my-notes-[0-9a-f]{8}$/)
    expect(TwiggNamespace.validate(namespace)).toBeUndefined()
  })

  test("directories that sanitise to the same slug stay apart", () => {
    const input = { projectID: "global", profile: "matti", device: "box" }
    const a = TwiggNamespace.namespaceFor({ ...input, directory: "/a/b-c" })
    const b = TwiggNamespace.namespaceFor({ ...input, directory: "/a-b/c" })
    expect(a).not.toBe(b)
  })

  test("long paths and names stay within the limits", () => {
    const namespace = TwiggNamespace.namespaceFor({
      projectID: "global",
      directory: "/" + "very-long-directory-name/".repeat(40),
      profile: "p".repeat(64),
      device: "d".repeat(200),
    })
    expect(TwiggNamespace.validate(namespace)).toBeUndefined()
    namespace.split("/").forEach((part) => expect(part.length).toBeLessThanOrEqual(64))
  })

  test("segment never returns an empty or invalid segment", () => {
    expect(TwiggNamespace.segment("")).toBe("none")
    expect(TwiggNamespace.segment("///")).toBe("none")
    expect(TwiggNamespace.segment("Ünïcode Host")).toBe("n-code-host")
    expect(TwiggNamespace.segment("ok_name-1")).toBe("ok_name-1")
  })

  test("validate", () => {
    expect(TwiggNamespace.validate("oc/matti/p/abc")).toBeUndefined()
    expect(TwiggNamespace.validate("oc/Matti")).toContain("may only contain")
    expect(TwiggNamespace.validate("oc//p")).toContain("empty segment")
    expect(TwiggNamespace.validate("oc/" + "a".repeat(65))).toContain("longer than 64 bytes")
    expect(TwiggNamespace.validate(Array.from({ length: 5 }, () => "a".repeat(60)).join("/"))).toContain(
      "longer than 255 bytes",
    )
  })
})

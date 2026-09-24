import type { JSONSchema7 } from "@ai-sdk/provider"
import { JsonSchema, Schema } from "effect"
import type * as Tool from "./tool"

type JsonObject = Record<string, unknown>
const cache = new WeakMap<Schema.Top, JSONSchema7>()

export function fromSchema(schema: Schema.Top): JSONSchema7 {
  const cached = cache.get(schema)
  if (cached) return cached

  const document = Schema.toJsonSchemaDocument(schema, { additionalProperties: true })
  const result = normalize({
    $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
    ...document.schema,
    ...(Object.keys(document.definitions).length > 0 ? { $defs: document.definitions } : {}),
  })
  const inlined = dropDefinitionsIfResolved(inlineLocalReferences(result))
  if (!isJsonSchema(inlined)) throw new Error("tool JSON Schema helper produced a non-schema value")
  cache.set(schema, inlined)
  return inlined
}

export function fromTool(tool: Tool.Def): JSONSchema7 {
  return tool.jsonSchema ?? fromSchema(tool.parameters as Schema.Top)
}

function normalize(value: unknown, options: { stripNull?: boolean } = {}): unknown {
  if (Array.isArray(value)) return value.map((item) => normalize(item))
  if (!isRecord(value)) return value

  const required = Array.isArray(value.required)
    ? new Set(value.required.filter((item) => typeof item === "string"))
    : undefined
  const schema = Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key,
      key === "properties" && isRecord(item)
        ? Object.fromEntries(
            Object.entries(item).map(([name, property]) => [
              name,
              normalize(property, { stripNull: !required?.has(name) }),
            ]),
          )
        : normalize(item),
    ]),
  )

  if (schema.additionalProperties === true) delete schema.additionalProperties

  if (options.stripNull && Array.isArray(schema.anyOf)) {
    const withoutNull = schema.anyOf.filter((item) => !isRecord(item) || item.type !== "null")
    if (withoutNull.length !== schema.anyOf.length) return normalize({ ...schema, anyOf: withoutNull })
  }

  if (Array.isArray(schema.anyOf)) {
    const withoutNull = schema.anyOf
    const number = withoutNull.find((item) => isRecord(item) && item.type === "number")
    const nonFinite = withoutNull.filter(
      (item) => isRecord(item) && Array.isArray(item.enum) && item.enum.every((entry) => isNonFiniteNumber(entry)),
    )
    if (number && nonFinite.length === withoutNull.length - 1) {
      const { anyOf: _, ...rest } = schema
      return normalize({ ...number, ...rest })
    }

    if (isEmptyStructUnion(withoutNull)) {
      const { anyOf: _, ...rest } = schema
      return normalize({ type: "object", properties: {}, ...rest })
    }

    if (withoutNull.length === 1 && isRecord(withoutNull[0])) {
      const { anyOf: _, ...rest } = schema
      return normalize({ ...withoutNull[0], ...rest })
    }
  }

  if (Array.isArray(schema.allOf) && schema.allOf.every(isRecord) && canFlattenAllOf(schema.allOf, schema)) {
    const { allOf, ...rest } = schema
    return normalize({ ...Object.assign({}, ...allOf), ...rest })
  }

  if (schema.type === "integer" && schema.maximum === undefined) {
    return { minimum: Number.MIN_SAFE_INTEGER, ...schema, maximum: Number.MAX_SAFE_INTEGER }
  }

  return schema
}

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isJsonSchema(value: unknown): value is JSONSchema7 {
  return typeof value === "boolean" || isRecord(value)
}

function isNonFiniteNumber(value: unknown) {
  return value === "NaN" || value === "Infinity" || value === "-Infinity"
}

function isEmptyStructUnion(items: unknown[]) {
  return (
    items.length === 2 &&
    items.some((item) => isRecord(item) && item.type === "object" && item.properties === undefined) &&
    items.some((item) => isRecord(item) && item.type === "array" && item.items === undefined)
  )
}

function canFlattenAllOf(allOf: JsonObject[], parent: JsonObject) {
  const keys = new Set(Object.keys(parent).filter((key) => key !== "allOf"))
  return allOf.every((item) =>
    Object.keys(item).every((key) => {
      if (keys.has(key)) return false
      keys.add(key)
      return true
    }),
  )
}

function inlineLocalReferences(value: unknown, definitions?: JsonObject, seen = new Set<string>()): unknown {
  if (Array.isArray(value)) return value.map((item) => inlineLocalReferences(item, definitions, seen))
  if (!isRecord(value)) return value

  const localDefinitions = definitions ?? (isRecord(value.$defs) ? value.$defs : undefined)
  if (typeof value.$ref === "string" && localDefinitions) {
    const name = value.$ref.match(/^#\/\$defs\/(.+)$/)?.[1] ?? value.$ref.match(/^#\/definitions\/(.+)$/)?.[1]
    if (name && !seen.has(name)) {
      const target = localDefinitions[name]
      if (target) {
        const { $ref: _, ...rest } = value
        return inlineLocalReferences(
          { ...(isRecord(target) ? target : {}), ...rest },
          localDefinitions,
          new Set(seen).add(name),
        )
      }
    }
  }

  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, inlineLocalReferences(item, localDefinitions, seen)]),
  )
}

function dropDefinitionsIfResolved(value: unknown): unknown {
  if (!isRecord(value) || hasLocalReference(value)) return value
  const { $defs: _, definitions: __, ...rest } = value
  return rest
}

function hasLocalReference(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(hasLocalReference)
  if (!isRecord(value)) return false
  if (
    typeof value.$ref === "string" &&
    (value.$ref.startsWith("#/$defs/") || value.$ref.startsWith("#/definitions/"))
  ) {
    return true
  }
  return Object.values(value).some(hasLocalReference)
}

// Twigg forwards tool schemas to the model's own provider, and some families reject parts of standard JSON Schema:
// Gemini (integer enums and more) and Kimi ($ref siblings, tuple items). Keyed on the model name, which Twigg's
// catalogue keeps (e.g. gemini-3.1-flash-lite).
export function forModel(model: { providerID: string; api: { id: string } }, schema: JSONSchema7): JSONSchema7 {
  if (model.providerID === "moonshotai" || model.api.id.toLowerCase().includes("kimi")) {
    const sanitizeMoonshot = (obj: unknown): unknown => {
      if (obj === null || typeof obj !== "object") return obj
      if (Array.isArray(obj)) return obj.map(sanitizeMoonshot)
      // Moonshot expands $ref before validation and rejects sibling keywords like description on the same node.
      if ("$ref" in obj && typeof obj.$ref === "string") return { $ref: obj.$ref }
      const result = Object.fromEntries(Object.entries(obj).map(([key, value]) => [key, sanitizeMoonshot(value)]))
      // MFJS does not support tuple-style `items` arrays; it requires one schema object for all array items.
      if (Array.isArray(result.items)) result.items = result.items[0] ?? {}
      return result
    }

    const sanitized = sanitizeMoonshot(schema)
    if (typeof sanitized === "object" && sanitized !== null && !Array.isArray(sanitized)) {
      schema = sanitized
    }
  }

  // Convert integer enums to string enums for Google/Gemini
  if (model.providerID === "google" || model.api.id.includes("gemini")) {
    const isPlainObject = (node: unknown): node is Record<string, any> =>
      typeof node === "object" && node !== null && !Array.isArray(node)
    const hasCombiner = (node: unknown) =>
      isPlainObject(node) && (Array.isArray(node.anyOf) || Array.isArray(node.oneOf) || Array.isArray(node.allOf))
    const hasSchemaIntent = (node: unknown) => {
      if (!isPlainObject(node)) return false
      if (hasCombiner(node)) return true
      return [
        "type",
        "properties",
        "items",
        "prefixItems",
        "enum",
        "const",
        "$ref",
        "additionalProperties",
        "patternProperties",
        "required",
        "not",
        "if",
        "then",
        "else",
      ].some((key) => key in node)
    }

    const sanitizeGemini = (obj: any): any => {
      if (obj === null || typeof obj !== "object") {
        return obj
      }

      if (Array.isArray(obj)) {
        return obj.map(sanitizeGemini)
      }

      const result: any = {}
      for (const [key, value] of Object.entries(obj)) {
        if (key === "enum" && Array.isArray(value)) {
          // Convert all enum values to strings
          result[key] = value.map((v) => String(v))
          // If we have integer type with enum, change type to string
          if (result.type === "integer" || result.type === "number") {
            result.type = "string"
          }
        } else if (typeof value === "object" && value !== null) {
          result[key] = sanitizeGemini(value)
        } else {
          result[key] = value
        }
      }

      // Gemini requires a single `type`, not a JSON Schema type array such as
      // `["number","string"]` (emitted by some MCP servers). Plain `@ai-sdk/google`
      // rewrites these into an `anyOf` of single-type schemas, but OpenAI-compatible
      // transports (e.g. GitHub Copilot proxying to Gemini) forward them verbatim
      // and the backend rejects the array form. Mirror the SDK: split non-null
      // types into `anyOf`, and lift `null` into `nullable`.
      if (Array.isArray(result.type)) {
        const hasNull = result.type.includes("null")
        const nonNull = result.type.filter((entry: unknown) => entry !== "null")
        if (nonNull.length === 0) {
          result.type = "null"
        } else {
          delete result.type
          result.anyOf = nonNull.map((entry: unknown) => ({ type: entry }))
          if (hasNull) result.nullable = true
        }
      }

      // Filter required array to only include fields that exist in properties
      if (result.type === "object" && result.properties && Array.isArray(result.required)) {
        result.required = result.required.filter((field: any) => field in result.properties)
      }

      if (result.type === "array" && !hasCombiner(result)) {
        if (result.items == null) {
          result.items = {}
        }
        // Ensure items has a type only when it's still schema-empty.
        if (isPlainObject(result.items) && !hasSchemaIntent(result.items)) {
          result.items.type = "string"
        }
      }

      // Remove properties/required from non-object types (Gemini rejects these)
      if (result.type && result.type !== "object" && !hasCombiner(result)) {
        delete result.properties
        delete result.required
      }

      return result
    }

    schema = sanitizeGemini(schema)
  }

  return schema
}

export * as ToolJsonSchema from "./json-schema"

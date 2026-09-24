export * as ConfigTwiggV1 from "./twigg"

import { Schema } from "effect"

export const Info = Schema.Struct({
  profile: Schema.optional(Schema.String.check(Schema.isPattern(/^[a-z0-9_-]{1,64}$/))).annotate({
    description:
      "Twigg profile name, the first segment of every chat namespace (oc/<profile>/...). Lowercase letters, digits, _ and -",
  }),
  device: Schema.optional(Schema.String).annotate({
    description: "Device name used in namespaces for folders without git. Defaults to the hostname",
  }),
  baseURL: Schema.optional(Schema.String).annotate({
    description: "Twigg API base URL. Defaults to https://api.twigg.ai/api/v1",
  }),
})
export type Info = Schema.Schema.Type<typeof Info>

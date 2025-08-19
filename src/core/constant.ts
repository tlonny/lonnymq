import { fromHours, fromSecs } from "@src/core/ms"
import { createHash } from "node:crypto"

export class DatabaseConstant {
    private readonly value: string

    constructor(value : string) {
        this.value = value
    }

    toString(schema: string): string {
        return createHash("sha256")
            .update(schema)
            .update(this.value)
            .digest("base64")
            .replace(/=/g, "")
    }
}

export const WAKE_CHANNEL = new DatabaseConstant("WAKE")
export const USE_WAKE_DEFAULT = false
export const DELAY_MS_DEFAULT = fromSecs(0)
export const LOCK_MS_DEFAULT = fromHours(1)

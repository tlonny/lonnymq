import type { DatabaseClient } from "@src/core/database"
import { ref, sql, value } from "@src/core/sql"

export class ChannelPolicySetCommand {

    readonly schema: string
    readonly channelName: string
    readonly maxSize: number | null
    readonly maxConcurrency: number | null
    readonly releaseIntervalMs: number | null
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        maxSize?: number | null,
        maxConcurrency?: number | null,
        releaseIntervalMs?: number | null
    }) {
        this.schema = params.schema
        this.channelName = params.channelName

        const maxConcurrency = params.maxConcurrency ?? null
        this.maxConcurrency = maxConcurrency !== null
            ? Math.max(0, maxConcurrency)
            : null

        const maxSize = params.maxSize ?? null
        this.maxSize = maxSize !== null
            ? Math.max(0, maxSize)
            : null

        const releaseIntervalMs = params.releaseIntervalMs ?? null
        this.releaseIntervalMs = releaseIntervalMs !== null
            ? Math.max(0, releaseIntervalMs)
            : null

        this.createdAt = new Date()
    }

    sortKeyGet(): string {
        return JSON.stringify([
            this.channelName,
            null,
            this.createdAt.toISOString(),
        ])
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        await databaseClient.query(sql`
            SELECT 1 FROM ${ref(this.schema)}."channel_policy_set"(
                ${value(this.channelName)},
                ${value(this.maxSize)}::INTEGER,
                ${value(this.maxConcurrency)}::INTEGER,
                ${value(this.releaseIntervalMs)}::INTEGER
            )
        `.value)
    }
}

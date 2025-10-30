import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

export class ChannelPolicySetCommand {

    readonly schema: string
    readonly channelId: string
    readonly maxConcurrency: number | null
    readonly maxSize: number | null
    readonly releaseIntervalMs: number | null
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelId: string,
        maxConcurrency?: number | null,
        maxSize?: number | null,
        releaseIntervalMs?: number | null
    }) {
        this.schema = params.schema
        this.channelId = params.channelId

        const maxConcurrency = params.maxConcurrency ?? null
        this.maxConcurrency = maxConcurrency !== null
            ? Math.max(1, maxConcurrency)
            : null

        const maxSize = params.maxSize ?? null
        this.maxSize = maxSize !== null
            ? Math.max(1, maxSize)
            : null

        const releaseIntervalMs = params.releaseIntervalMs ?? null
        this.releaseIntervalMs = releaseIntervalMs !== null
            ? Math.max(0, releaseIntervalMs)
            : null

        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        await databaseClient.query(sql`
            SELECT 1 FROM ${ref(this.schema)}."channel_policy_set"(
                $1,
                $2::INTEGER,
                $3::INTEGER,
                $4::INTEGER
            )
        `.value, [
            this.channelId,
            this.maxConcurrency,
            this.maxSize,
            this.releaseIntervalMs
        ])
    }
}

import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

export class ChannelPolicySetCommand {

    readonly schema: string
    readonly channelName: string
    readonly maxConcurrency: number | null
    readonly releaseIntervalMs: number | null
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        maxConcurrency?: number | null,
        releaseIntervalMs?: number | null
    }) {
        this.schema = params.schema
        this.channelName = params.channelName

        const maxConcurrency = params.maxConcurrency ?? null
        this.maxConcurrency = maxConcurrency !== null
            ? Math.max(0, maxConcurrency)
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
                $3::INTEGER
            )
        `.value, [
            this.channelName,
            this.maxConcurrency,
            this.releaseIntervalMs
        ])
    }
}

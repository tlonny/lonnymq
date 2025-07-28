import type { DatabaseClient } from "@src/core/database"
import { ref, sql, value } from "@src/core/sql"

export class ChannelPolicySetCommand {

    readonly schema: string
    readonly channelName: string
    readonly maxSize: number | null
    readonly maxConcurrency: number | null
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        maxSize: number | null,
        maxConcurrency: number | null
    }) {
        this.schema = params.schema
        this.channelName = params.channelName

        this.maxConcurrency = params.maxConcurrency === null
            ? null
            : Math.max(0, params.maxConcurrency)

        this.maxSize = params.maxSize === null
            ? null
            : Math.max(0, params.maxSize)

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
                ${value(this.maxSize)}::BIGINT,
                ${value(this.maxConcurrency)}::BIGINT
            )
        `.value)
    }
}

import type { DatabaseClient } from "@src/core/database"
import { ref, sql, value } from "@src/core/sql"

export class ChannelPolicyClearCommand {

    readonly schema: string
    readonly channelName: string
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string
    }) {
        this.schema = params.schema
        this.channelName = params.channelName
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
            SELECT 1 FROM ${ref(this.schema)}."channel_policy_clear"(
                ${value(this.channelName)}
            )
        `.value)
    }
}

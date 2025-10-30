import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

export class ChannelPolicyClearCommand {

    readonly schema: string
    readonly channelId: string
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelId: string
    }) {
        this.schema = params.schema
        this.channelId = params.channelId
        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        await databaseClient.query(sql`
            SELECT 1 FROM ${ref(this.schema)}."channel_policy_clear"(
                $1
            )
        `.value, [
            this.channelId
        ])
    }
}

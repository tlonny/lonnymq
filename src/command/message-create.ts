import { DELAY_MS_DEFAULT } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { randomSlug } from "@src/core/random"
import { ref, sql } from "@src/core/sql"
import { randomUUID } from "node:crypto"

export class MessageCreateCommand {

    readonly schema: string
    readonly channelName: string
    readonly content: Buffer
    readonly id: string
    readonly delayMs: number
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName?: string,
        content: Buffer,
        delayMs?: number,
    }) {
        const delayMs = params.delayMs === undefined
            ? DELAY_MS_DEFAULT
            : params.delayMs

        this.id = randomUUID()
        this.schema = params.schema
        this.channelName = params.channelName ?? randomSlug()
        this.content = params.content
        this.delayMs = delayMs
        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        await databaseClient.query(sql`
            SELECT 1 FROM ${ref(this.schema)}."message_create"(
                $1, 
                $2, 
                $3,
                $4::BIGINT
            )
        `.value, [
            this.id,
            this.channelName,
            this.content,
            this.delayMs
        ])
    }
}

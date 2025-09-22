import { DELAY_MS_DEFAULT } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { randomUUID } from "node:crypto"

export class MessageCreateCommand {

    private readonly schema: string

    readonly channelName: string
    readonly content: Buffer
    readonly lockMs: number
    readonly id: string
    readonly delayMs: number
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        content: Buffer,
        lockMs: number,
        delayMs?: number,
    }) {
        const lockMs = Math.max(0, params.lockMs)
        const delayMs = params.delayMs === undefined
            ? DELAY_MS_DEFAULT
            : params.delayMs

        this.id = randomUUID()
        this.schema = params.schema
        this.channelName = params.channelName
        this.content = params.content
        this.lockMs = lockMs
        this.delayMs = delayMs
        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        await databaseClient.query(sql`
            SELECT 1 FROM ${ref(this.schema)}."message_create"(
                $1, 
                $2, 
                $3,
                $4::BIGINT, 
                $5::BIGINT
            )
        `.value, [
            this.id,
            this.channelName,
            this.content,
            this.lockMs,
            this.delayMs
        ])
    }
}

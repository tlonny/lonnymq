import { DELAY_MS_DEFAULT, MessageCreateResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { randomSlug } from "@src/core/random"
import { ref, sql } from "@src/core/sql"

type QueryResult = {
    result_code: MessageCreateResultCode.MESSAGE_CREATED,
    metadata: { id: string, channel_size: number }
}

export type MessageCreateCommandResult = {
    resultType: "MESSAGE_CREATED",
    id: string,
    channelSize: number,
}

export class MessageCreateCommand {

    readonly schema: string
    readonly channelName: string
    readonly content: Buffer
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

        this.schema = params.schema
        this.channelName = params.channelName ?? randomSlug()
        this.content = params.content
        this.delayMs = delayMs
        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageCreateCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT 
                result_code, 
                metadata 
            FROM ${ref(this.schema)}."message_create"(
                $1, 
                $2,
                $3::BIGINT
            )
        `.value, [
            this.channelName,
            this.content,
            this.delayMs
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageCreateResultCode.MESSAGE_CREATED) {
            return {
                resultType: "MESSAGE_CREATED",
                id: result.metadata.id,
                channelSize: result.metadata.channel_size,
            }
        } else {
            throw new Error("Unexpected result code")
        }
    }
}

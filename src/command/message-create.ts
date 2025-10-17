import { MessageCreateResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResult = {
    result_code: MessageCreateResultCode.MESSAGE_CREATED,
    metadata: { id: string, channel_size: number }
}

export type MessageCreateCommandResult = {
    resultType: "MESSAGE_CREATED",
    id: bigint,
    channelSize: number,
}

export class MessageCreateCommand {

    readonly schema: string
    readonly channelName: string
    readonly content: Buffer
    readonly offsetMs: number | null
    readonly timestamp: number | null

    constructor(params: {
        schema: string,
        channelName: string,
        content: Buffer,
        offsetMs: number | null,
        timestamp: number | null
    }) {
        this.schema = params.schema
        this.channelName = params.channelName
        this.content = params.content
        this.offsetMs = params.offsetMs
        this.timestamp = params.timestamp
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageCreateCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT 
                result_code, 
                metadata 
            FROM ${ref(this.schema)}."message_create"(
                $1, 
                $2,
                $3::BIGINT,
                $4::BIGINT
            )
        `.value, [
            this.channelName,
            this.content,
            this.timestamp,
            this.offsetMs
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageCreateResultCode.MESSAGE_CREATED) {
            return {
                resultType: "MESSAGE_CREATED",
                id: BigInt(result.metadata.id),
                channelSize: result.metadata.channel_size,
            }
        } else {
            throw new Error("Unexpected result code")
        }
    }
}

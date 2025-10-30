import { MessageCreateResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResultMessageCreated = {
    result_code: MessageCreateResultCode.MESSAGE_CREATED,
    metadata: { id: string, channel_size: number }
}

type QueryResultMessageDropped = {
    result_code: MessageCreateResultCode.MESSAGE_DROPPED,
    metadata: null
}

type QueryResult =
    | QueryResultMessageCreated
    | QueryResultMessageDropped

export type MessageCreateCommandResultMessageCreated = {
    resultType: "MESSAGE_CREATED",
    id: bigint,
    channelSize: number
}

export type MessageCreateCommandResultMessageDropped = {
    resultType: "MESSAGE_DROPPED"
}

export type MessageCreateCommandResult =
    | MessageCreateCommandResultMessageCreated
    | MessageCreateCommandResultMessageDropped

export class MessageCreateCommand {

    readonly schema: string
    readonly channelId: string
    readonly content: Buffer
    readonly dequeueAt: number | null

    constructor(params: {
        schema: string,
        channelId: string,
        content: Buffer,
        dequeueAt: number | null
    }) {
        this.schema = params.schema
        this.channelId = params.channelId
        this.content = params.content
        this.dequeueAt = params.dequeueAt
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
            this.channelId,
            this.content,
            this.dequeueAt
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageCreateResultCode.MESSAGE_CREATED) {
            return {
                resultType: "MESSAGE_CREATED",
                id: BigInt(result.metadata.id),
                channelSize: result.metadata.channel_size,
            }
        } else if (result.result_code === MessageCreateResultCode.MESSAGE_DROPPED) {
            return {
                resultType: "MESSAGE_DROPPED"
            }
        } else {
            result satisfies never
            throw new Error("Unexpected result code")
        }
    }
}

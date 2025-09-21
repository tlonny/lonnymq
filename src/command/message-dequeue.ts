import { MessageDequeueResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResultMessageNotAvailable = {
    result_code: MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE,
    content: null,
    state: null,
    metadata: {
        retry_ms: number | null
    },
}

type QueryResultMessageDequeued = {
    result_code: MessageDequeueResultCode.MESSAGE_DEQUEUED,
    state: Buffer | null,
    content: Buffer,
    metadata: {
        id: string,
        channel_name: string,
        is_unlocked: boolean,
        name: string | null,
        num_attempts: number
    }
}

type QueryResult =
    | QueryResultMessageNotAvailable
    | QueryResultMessageDequeued

export type MessageDequeueCommandResultMessageDequeued = {
    resultType: "MESSAGE_DEQUEUED",
    message: {
        id: string,
        channelName: string,
        isUnlocked: boolean,
        name: string | null,
        content: Buffer,
        state: Buffer | null,
        numAttempts: number,
    }
}

export type MessageDequeueCommandResultMessageNotAvailable = {
    resultType: "MESSAGE_NOT_AVAILABLE",
    retryMs: number | null
}

export type MessageDequeueCommandResult =
    | MessageDequeueCommandResultMessageDequeued
    | MessageDequeueCommandResultMessageNotAvailable

export class MessageDequeueCommand {

    private readonly schema: string

    constructor(params: {
        schema: string,
    }) {
        this.schema = params.schema
    }

    async execute(databaseClient: DatabaseClient) : Promise<MessageDequeueCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT
                result_code,
                metadata,
                content,
                state
            FROM ${ref(this.schema)}."message_dequeue"()
        `.value, []).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE) {
            return {
                resultType: "MESSAGE_NOT_AVAILABLE",
                retryMs: result.metadata.retry_ms
            }
        } else if (result.result_code === MessageDequeueResultCode.MESSAGE_DEQUEUED) {
            return {
                resultType: "MESSAGE_DEQUEUED",
                message: {
                    id: result.metadata.id,
                    channelName: result.metadata.channel_name,
                    isUnlocked: result.metadata.is_unlocked,
                    name: result.metadata.name,
                    content: result.content,
                    state: result.state,
                    numAttempts: result.metadata.num_attempts
                }
            }
        } else {
            throw new Error("Unexpected dequeue result")
        }
    }

}

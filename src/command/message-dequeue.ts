import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { MessageDequeueResultCode } from "@src/migration/05-function-message-dequeue"

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
        name: string | null,
        dequeue_nonce: string,
        num_attempts: number
    }
}

type QueryResult =
    | QueryResultMessageNotAvailable
    | QueryResultMessageDequeued

export type MessageDequeueCommandResultMessageDequeued = {
    resultType: "MESSAGE_DEQUEUED",
    message: {
        id: bigint,
        channelName: string,
        name: string | null,
        content: Buffer,
        dequeueNonce: string,
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

    readonly schema: string

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
                    id: BigInt(result.metadata.id),
                    channelName: result.metadata.channel_name,
                    name: result.metadata.name,
                    content: result.content,
                    dequeueNonce: result.metadata.dequeue_nonce,
                    state: result.state,
                    numAttempts: result.metadata.num_attempts,
                }
            }
        } else {
            throw new Error("Unexpected dequeue result")
        }
    }

}

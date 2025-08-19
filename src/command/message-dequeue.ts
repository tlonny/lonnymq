import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { MessageDequeueResultCode } from "@src/migration/05-function-message-dequeue"

type QueryResultMessageNotAvailable = {
    result_code: MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE,
    retry_ms: number | null
}

type QueryResultMessageDequeued = {
    result_code: MessageDequeueResultCode.MESSAGE_DEQUEUED,
    id: string,
    channel_name: string,
    name: string | null,
    content: string,
    dequeue_nonce: string,
    state: string | null,
    num_attempts: number
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
        content: string,
        dequeueNonce: string,
        state: string | null,
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
            SELECT ${ref(this.schema)}."message_dequeue"() AS "result"
        `.value).then(res => res.rows[0].result as QueryResult)

        if (result.result_code === MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE) {
            return {
                resultType: "MESSAGE_NOT_AVAILABLE",
                retryMs: result.retry_ms
            }
        } else if (result.result_code === MessageDequeueResultCode.MESSAGE_DEQUEUED) {
            return {
                resultType: "MESSAGE_DEQUEUED",
                message: {
                    id: BigInt(result.id),
                    channelName: result.channel_name,
                    name: result.name,
                    content: result.content,
                    dequeueNonce: result.dequeue_nonce,
                    state: result.state,
                    numAttempts: result.num_attempts,
                }
            }
        } else {
            throw new Error("Unexpected dequeue result")
        }
    }

}

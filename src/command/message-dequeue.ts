import { MessageDequeueResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResultMessageNotAvailable = {
    result_code: MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE,
    content: null,
    state: null,
    metadata: null
}

type QueryResultMessageDequeued = {
    result_code: MessageDequeueResultCode.MESSAGE_DEQUEUED,
    state: Buffer | null,
    content: Buffer,
    metadata: {
        id: string,
        channel_name: string,
        is_unlocked: boolean,
        num_attempts: number
    }
}

type QueryResult =
    | QueryResultMessageNotAvailable
    | QueryResultMessageDequeued

type MessageDequeueCommandResultMessageDequeued = {
    resultType: "MESSAGE_DEQUEUED",
    id: bigint,
    channelName: string,
    isUnlocked: boolean,
    content: Buffer,
    state: Buffer | null,
    numAttempts: number,
}

type MessageDequeueCommandResultMessageNotAvailable = {
    resultType: "MESSAGE_NOT_AVAILABLE"
}

type MessageDequeueCommandResult =
    | MessageDequeueCommandResultMessageDequeued
    | MessageDequeueCommandResultMessageNotAvailable

export class MessageDequeueCommand {

    readonly schema: string
    readonly lockMs: number

    constructor(params: {
        schema: string,
        lockMs: number,
    }) {
        this.schema = params.schema
        this.lockMs = params.lockMs
    }

    async execute(databaseClient: DatabaseClient) : Promise<MessageDequeueCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT
                result_code,
                metadata,
                content,
                state
            FROM ${ref(this.schema)}."message_dequeue"($1::BIGINT)
        `.value, [
            this.lockMs
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE) {
            return { resultType: "MESSAGE_NOT_AVAILABLE" }
        } else if (result.result_code === MessageDequeueResultCode.MESSAGE_DEQUEUED) {
            return {
                resultType: "MESSAGE_DEQUEUED",
                id: BigInt(result.metadata.id),
                channelName: result.metadata.channel_name,
                isUnlocked: result.metadata.is_unlocked,
                content: result.content,
                state: result.state,
                numAttempts: result.metadata.num_attempts
            }
        } else {
            throw new Error("Unexpected dequeue result")
        }
    }

}

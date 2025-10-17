import { MessageDeferResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResult =
    | { result_code: MessageDeferResultCode.MESSAGE_NOT_FOUND}
    | { result_code: MessageDeferResultCode.MESSAGE_STATE_INVALID }
    | { result_code: MessageDeferResultCode.MESSAGE_DEFERRED }

export type MessageDeferCommandResultMessageNotFound = {
    resultType: "MESSAGE_NOT_FOUND"
}

export type MessageDeferCommandResultStateInvalid = {
    resultType: "STATE_INVALID"
}

export type MessageDeferCommandResultMessageDeferred = {
    resultType: "MESSAGE_DEFERRED"
}

export type MessageDeferCommandResult =
    | MessageDeferCommandResultMessageNotFound
    | MessageDeferCommandResultStateInvalid
    | MessageDeferCommandResultMessageDeferred

export class MessageDeferCommand {

    readonly schema: string
    readonly id: bigint
    readonly numAttempts: number
    readonly state: Buffer | null
    readonly offsetMs: number | null
    readonly timestamp: number | null

    constructor(params: {
        schema: string,
        id: bigint,
        numAttempts: number,
        state: Buffer | null
        offsetMs: number | null,
        timestamp: number | null
    }) {
        this.schema = params.schema
        this.numAttempts = params.numAttempts
        this.id = params.id
        this.state = params.state
        this.offsetMs = params.offsetMs
        this.timestamp = params.timestamp
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageDeferCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT * FROM ${ref(this.schema)}."message_defer"(
                $1::BIGINT,
                $2::BIGINT,
                $3::BIGINT,
                $4::BIGINT,
                $5
            )
        `.value, [
            this.id.toString(),
            this.numAttempts,
            this.timestamp,
            this.offsetMs,
            this.state
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageDeferResultCode.MESSAGE_NOT_FOUND) {
            return { resultType: "MESSAGE_NOT_FOUND" }
        } else if (result.result_code === MessageDeferResultCode.MESSAGE_STATE_INVALID) {
            return { resultType: "STATE_INVALID" }
        } else if (result.result_code === MessageDeferResultCode.MESSAGE_DEFERRED) {
            return { resultType: "MESSAGE_DEFERRED" }
        } else {
            result satisfies never
            throw new Error("Unexpected result")
        }
    }
}

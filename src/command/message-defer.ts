import { DELAY_MS_DEFAULT } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { MessageDeferResultCode } from "@src/migration/07-function-message-defer"

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
    readonly id: string
    readonly delayMs: number
    readonly state: Buffer | null

    constructor(params: {
        schema: string,
        id: string,
        delayMs?: number,
        state?: Buffer | null
    }) {
        const delayMs = params.delayMs === undefined
            ? DELAY_MS_DEFAULT
            : params.delayMs

        this.schema = params.schema
        this.id = params.id
        this.delayMs = delayMs
        this.state = params.state ?? null
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageDeferCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT * FROM ${ref(this.schema)}."message_defer"(
                $1,
                $2,
                $3
            )
        `.value, [
            this.id,
            this.delayMs,
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

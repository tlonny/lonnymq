import { DELAY_MS_DEFAULT } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql, value } from "@src/core/sql"
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
    readonly id: bigint
    readonly dequeueNonce: string
    readonly delayMs: number
    readonly state: string | null

    constructor(params: {
        schema: string,
        id: bigint,
        dequeueNonce: string,
        delayMs?: number,
        state?: string | null
    }) {
        const delayMs = params.delayMs === undefined
            ? DELAY_MS_DEFAULT
            : params.delayMs

        this.schema = params.schema
        this.id = params.id
        this.dequeueNonce = params.dequeueNonce
        this.delayMs = delayMs
        this.state = params.state ?? null
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageDeferCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT ${ref(this.schema)}."message_defer"(
                ${value(this.id)},
                ${value(this.dequeueNonce)},
                ${value(this.delayMs)},
                ${value(this.state)}
            ) AS "result"
        `.value).then(res => res.rows[0].result as QueryResult)

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

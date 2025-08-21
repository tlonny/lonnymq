import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { MessageDeleteResultCode } from "@src/migration/06-function-message-delete"

type QueryResult =
    | { result_code: MessageDeleteResultCode.MESSAGE_NOT_FOUND}
    | { result_code: MessageDeleteResultCode.MESSAGE_STATE_INVALID }
    | { result_code: MessageDeleteResultCode.MESSAGE_DELETED }

export type MessageDeleteCommandResultMessageNotFound = {
    resultType: "MESSAGE_NOT_FOUND"
}

export type MessageDeleteCommandResultStateInvalid = {
    resultType: "STATE_INVALID"
}

export type MessageDeleteCommandResultMessageDeleted = {
    resultType: "MESSAGE_DELETED"
}

export type MessageDeleteCommandResult =
    | MessageDeleteCommandResultMessageNotFound
    | MessageDeleteCommandResultStateInvalid
    | MessageDeleteCommandResultMessageDeleted

export class MessageDeleteCommand {

    readonly schema: string
    readonly id: bigint
    readonly dequeueNonce: string

    constructor(params: {
        schema: string,
        id: bigint,
        dequeueNonce: string,
    }) {
        this.schema = params.schema
        this.id = params.id
        this.dequeueNonce = params.dequeueNonce
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageDeleteCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT * FROM ${ref(this.schema)}."message_delete"(
                $1,
                $2
            )
        `.value, [
            this.id,
            this.dequeueNonce
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageDeleteResultCode.MESSAGE_NOT_FOUND) {
            return { resultType: "MESSAGE_NOT_FOUND" }
        } else if (result.result_code === MessageDeleteResultCode.MESSAGE_STATE_INVALID) {
            return { resultType: "STATE_INVALID" }
        } else if (result.result_code === MessageDeleteResultCode.MESSAGE_DELETED) {
            return { resultType: "MESSAGE_DELETED" }
        } else {
            result satisfies never
            throw new Error("Unexpected result")
        }
    }

}

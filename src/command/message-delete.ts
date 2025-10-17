import { MessageDeleteResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

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
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        numAttempts: number,
        id: bigint,
    }) {
        this.schema = params.schema
        this.id = params.id
        this.numAttempts = params.numAttempts
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageDeleteCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT * FROM ${ref(this.schema)}."message_delete"(
                $1::BIGINT,
                $2::BIGINT
            )
        `.value, [
            this.id.toString(),
            this.numAttempts
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

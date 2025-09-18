import { MessageHeartbeatResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"

type QueryResult =
    | { result_code: MessageHeartbeatResultCode.MESSAGE_NOT_FOUND}
    | { result_code: MessageHeartbeatResultCode.MESSAGE_STATE_INVALID }
    | { result_code: MessageHeartbeatResultCode.MESSAGE_HEARTBEATED }

export type MessageHeartbeatCommandResultMessageNotFound = {
    resultType: "MESSAGE_NOT_FOUND"
}

export type MessageHeartbeatCommandResultStateInvalid = {
    resultType: "MESSAGE_STATE_INVALID"
}

export type MessageHeartbeatCommandResultMessageHeartbeated = {
    resultType: "MESSAGE_HEARTBEATED"
}

export type MessageHeartbeatCommandResult =
    | MessageHeartbeatCommandResultMessageNotFound
    | MessageHeartbeatCommandResultStateInvalid
    | MessageHeartbeatCommandResultMessageHeartbeated

export class MessageHeartbeatCommand {
    readonly schema: string
    readonly id: string
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        id: string,
        numAttempts: number,
    }) {
        this.schema = params.schema
        this.numAttempts = params.numAttempts
        this.id = params.id
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageHeartbeatCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT * FROM ${ref(this.schema)}."message_heartbeat"(
                $1,
                $2::BIGINT
            )
        `.value, [
            this.id,
            this.numAttempts
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageHeartbeatResultCode.MESSAGE_NOT_FOUND) {
            return { resultType: "MESSAGE_NOT_FOUND" }
        } else if (result.result_code === MessageHeartbeatResultCode.MESSAGE_STATE_INVALID) {
            return { resultType: "MESSAGE_STATE_INVALID" }
        } else if (result.result_code === MessageHeartbeatResultCode.MESSAGE_HEARTBEATED) {
            return { resultType: "MESSAGE_HEARTBEATED" }
        } else {
            result satisfies never
            throw new Error("Unexpected result")
        }
    }
}

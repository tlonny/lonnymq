import { LOCK_MS_DEFAULT, PRIORITY_REDUCTION_MS, DELAY_MS_DEFAULT } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql, value } from "@src/core/sql"
import { MessageCreateResultCode } from "@src/migration/04-function-message-create"

type QueryResult =
    | { result_code: MessageCreateResultCode.MESSAGE_DROPPED }
    | { result_code: MessageCreateResultCode.MESSAGE_DEDUPLICATED, id: string }
    | { result_code: MessageCreateResultCode.MESSAGE_CREATED, id: string }

export type MessageCreateCommandResultMessageCreated = {
    resultType: "MESSAGE_CREATED",
    id: string
}

export type MessageCreateCommandResultMessageDeduplicated = {
    resultType: "MESSAGE_DEDUPLICATED",
    id: string
}

export type MessageCreateCommandResultMessageDropped = {
    resultType: "MESSAGE_DROPPED"
}

export type MessageCreateCommandResult =
    | MessageCreateCommandResultMessageCreated
    | MessageCreateCommandResultMessageDeduplicated
    | MessageCreateCommandResultMessageDropped

export class MessageCreateCommand {

    readonly schema: string
    readonly channelName: string
    readonly name: string | null
    readonly content: string
    readonly lockMs: number
    readonly delayMs: number
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        name?: string,
        content: string,
        lockMs?: number,
        delayMs?: number,
        priority?: boolean
    }) {
        const name = params.name ?? null
        const lockMs = params.lockMs === undefined
            ? LOCK_MS_DEFAULT
            : Math.max(0, params.lockMs)

        let delayMs : number
        if (params.priority) {
            delayMs = -PRIORITY_REDUCTION_MS
        } else {
            delayMs = params.delayMs === undefined
                ? DELAY_MS_DEFAULT
                : Math.max(0, params.delayMs)
        }

        this.schema = params.schema
        this.channelName = params.channelName
        this.content = params.content
        this.name = name
        this.lockMs = lockMs
        this.delayMs = delayMs
        this.createdAt = new Date()
    }

    async execute(databaseClient: DatabaseClient): Promise<MessageCreateCommandResult> {
        const result = await databaseClient.query(sql`
            SELECT ${ref(this.schema)}."message_create"(
                ${value(this.channelName)},
                ${value(this.name)},
                ${value(this.content)},
                ${value(this.lockMs)}::BIGINT,
                ${value(this.delayMs)}::BIGINT
            ) AS "result"
        `.value).then(res => res.rows[0].result as QueryResult)

        if (result.result_code === MessageCreateResultCode.MESSAGE_DROPPED) {
            return { resultType: "MESSAGE_DROPPED" }
        } else if (result.result_code === MessageCreateResultCode.MESSAGE_DEDUPLICATED) {
            return { resultType: "MESSAGE_DEDUPLICATED", id: result.id }
        } else if (result.result_code === MessageCreateResultCode.MESSAGE_CREATED) {
            return { resultType: "MESSAGE_CREATED", id: result.id }
        } else {
            result satisfies never
            throw new Error("Unexpected result")
        }
    }
}

import { DELAY_MS_DEFAULT, MessageCreateResultCode } from "@src/core/constant"
import type { DatabaseClient } from "@src/core/database"
import { ref, sql } from "@src/core/sql"
import { randomUUID } from "node:crypto"

type QueryResultMessageDropped = {
    result_code: MessageCreateResultCode.MESSAGE_DROPPED
}

type QueryResultMessageDeduplicated = {
    result_code: MessageCreateResultCode.MESSAGE_DEDUPLICATED
}

type QueryResultMessageCreated = {
    result_code: MessageCreateResultCode.MESSAGE_CREATED
}

type QueryResult =
    | QueryResultMessageDropped
    | QueryResultMessageDeduplicated
    | QueryResultMessageCreated

export type MessageCreateCommandResultMessageCreated = {
    resultType: "MESSAGE_CREATED"
}

export type MessageCreateCommandResultMessageDeduplicated = {
    resultType: "MESSAGE_DEDUPLICATED"
}

export type MessageCreateCommandResultMessageDropped = {
    resultType: "MESSAGE_DROPPED"
}

export type MessageCreateCommandResult =
    | MessageCreateCommandResultMessageCreated
    | MessageCreateCommandResultMessageDeduplicated
    | MessageCreateCommandResultMessageDropped

export class MessageCreateCommand {

    private readonly schema: string

    readonly channelName: string
    readonly name: string | null
    readonly content: Buffer
    readonly lockMs: number
    readonly id: string
    readonly delayMs: number
    readonly createdAt: Date

    constructor(params: {
        schema: string,
        channelName: string,
        name?: string,
        content: Buffer,
        lockMs: number,
        delayMs?: number,
    }) {
        const name = params.name ?? null
        const lockMs = Math.max(0, params.lockMs)
        const delayMs = params.delayMs === undefined
            ? DELAY_MS_DEFAULT
            : params.delayMs

        this.id = randomUUID()
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
            SELECT * FROM ${ref(this.schema)}."message_create"(
                $1, 
                $2, 
                $3, 
                $4,
                $5::BIGINT, 
                $6::BIGINT
            )
        `.value, [
            this.id,
            this.channelName,
            this.name,
            this.content,
            this.lockMs,
            this.delayMs
        ]).then(res => res.rows[0] as QueryResult)

        if (result.result_code === MessageCreateResultCode.MESSAGE_DROPPED) {
            return { resultType: "MESSAGE_DROPPED" }
        } else if (result.result_code === MessageCreateResultCode.MESSAGE_DEDUPLICATED) {
            return { resultType: "MESSAGE_DEDUPLICATED" }
        } else if (result.result_code === MessageCreateResultCode.MESSAGE_CREATED) {
            return { resultType: "MESSAGE_CREATED" }
        } else {
            result satisfies never
            throw new Error("Unexpected result")
        }
    }
}

import type { DatabaseClient } from "@src/core/database"
import { MessageCreateCommand, type MessageCreateCommandResult } from "@src/command/message-create"

export interface MessageCreateResultFuture {
    get(): Promise<MessageCreateCommandResult>
}

export class QueueChannelMessage {

    private readonly schema: string
    private readonly channelName: string

    constructor(params: {
        schema: string,
        channelName: string,
    }) {
        this.schema = params.schema
        this.channelName = params.channelName
    }

    async create(params : {
        databaseClient: DatabaseClient,
        name?: string,
        lockMs?: number,
        content: string,
        delayMs?: number,
    }) : Promise<MessageCreateCommandResult> {
        return new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            name: params.name,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        }).execute(params.databaseClient)
    }
}

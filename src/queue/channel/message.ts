import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand, type MessageCreateCommandResult } from "@src/command/message-create"

export interface MessageCreateResultFuture {
    get(): Promise<MessageCreateCommandResult>
}

export class QueueChannelMessage<T> {

    private readonly schema: string
    private readonly channelName: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelName: string,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.channelName = params.channelName
    }

    async create(params : {
        databaseClient: T,
        name?: string,
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) : Promise<MessageCreateCommandResult> {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            name: params.name,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        }).execute(adaptedClient)
    }
}

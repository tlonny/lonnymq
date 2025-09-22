import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

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

    create(params : {
        databaseClient: T,
        name?: string,
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        }).execute(adaptedClient)
    }
}

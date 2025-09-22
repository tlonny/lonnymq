import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

export class QueueChannelMessageModule<T> {

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
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        const command = new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        })

        await command.execute(adaptedClient)

        return { messageId: command.id }
    }
}

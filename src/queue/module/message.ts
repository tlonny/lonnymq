import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

export class QueueMessageModule<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
    }

    async create(params : {
        databaseClient: T,
        content: Buffer,
        delayMs?: number,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const command = new MessageCreateCommand({
            schema: this.schema,
            content: params.content,
            delayMs: params.delayMs,
        })

        const result = await command.execute(adaptedClient)
        return {
            messageId: result.metadata.id,
            channelName: command.channelName,
            channelSize: result.metadata.channelSize,
        }
    }
}


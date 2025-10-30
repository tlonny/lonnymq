import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

export class QueueChannelMessageModule<T> {

    private readonly schema: string
    private readonly channelId: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelId: string,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.channelId = params.channelId
    }

    async create(params : {
        databaseClient: T,
        content: Buffer,
        dequeueAt?: number
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const command = new MessageCreateCommand({
            schema: this.schema,
            channelId: this.channelId,
            content: params.content,
            dequeueAt: params.dequeueAt ?? null,
        })

        return await command.execute(adaptedClient)
    }
}

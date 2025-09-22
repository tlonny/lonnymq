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
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const command = new MessageCreateCommand({
            schema: this.schema,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        })

        await command.execute(adaptedClient)

        return {
            messageId: command.id,
            channelName: command.channelName
        }
    }
}


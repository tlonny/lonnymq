import { randomUUID } from "crypto"
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
        dequeueAt?: number
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const command = new MessageCreateCommand({
            schema: this.schema,
            content: params.content,
            dequeueAt: params.dequeueAt ?? null,
            channelId: randomUUID(),
        })

        return await command.execute(adaptedClient)
    }
}


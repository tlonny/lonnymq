import { type DatabaseClient, type DatabaseClientAdaptor } from "@src/core/database"
import { QueueBatchChannelModule } from "@src/queue/batch/module/channel"
import { QueueBatchMessageModule } from "@src/queue/batch/module/message"

type BatchedCommand = {
    sortKey: string,
    execute: (databaseClient: DatabaseClient) => Promise<void>
}

export type BatchedCommandRegisterFn = (command: BatchedCommand) => void

const compareFn = (a: BatchedCommand, b: BatchedCommand): number => {
    return a.sortKey.localeCompare(b.sortKey)
}

export class QueueBatch<T> {

    private readonly commands: BatchedCommand[]
    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    readonly message : QueueBatchMessageModule

    constructor(params : {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.commands = []

        this.message = new QueueBatchMessageModule({
            schema: this.schema,
            registerFn: (c) => this.commands.push(c)
        })
    }

    channel(channelName: string): QueueBatchChannelModule {
        return new QueueBatchChannelModule({
            schema: this.schema,
            channelName: channelName,
            registerFn: (c) => this.commands.push(c)
        })
    }

    async execute(params : {
        databaseClient: T
    }): Promise<void> {
        const adaptedClient = this.adaptor(params.databaseClient)
        for (const command of this.commands.sort(compareFn)) {
            await command.execute(adaptedClient)
        }
    }
}

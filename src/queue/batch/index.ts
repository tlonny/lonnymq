import { type DatabaseClient } from "@src/core/database"
import { QueueBatchChannel } from "@src/queue/batch/channel"

type BatchedCommand = {
    sortKey: string,
    execute: (databaseClient: DatabaseClient) => Promise<void>
}

export type BatchedCommandRegisterFn = (command: BatchedCommand) => void

const compareFn = (a: BatchedCommand, b: BatchedCommand): number => {
    return a.sortKey.localeCompare(b.sortKey)
}

export class QueueBatch {

    private readonly commands: BatchedCommand[]
    private readonly schema: string

    constructor(params : {
        schema: string,
    }) {
        this.commands = []
        this.schema = params.schema
    }

    channel(channelName: string): QueueBatchChannel {
        return new QueueBatchChannel({
            schema: this.schema,
            channelName: channelName,
            registerFn: (command: BatchedCommand) => {
                this.commands.push(command)
            }
        })
    }

    async execute(params : {
        databaseClient: DatabaseClient
    }): Promise<void> {
        for (const command of this.commands.sort(compareFn)) {
            await command.execute(params.databaseClient)
        }
    }
}

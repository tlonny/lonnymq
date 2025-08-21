import { MessageCreateCommand, type MessageCreateCommandResult } from "@src/command/message-create"
import { Deferred } from "@src/core/deferred"
import type { BatchedCommandRegisterFn } from "@src/queue/batch"

export class QueueBatchChannelMessage {

    private readonly schema: string
    private readonly channelName: string
    private readonly registerFn: BatchedCommandRegisterFn

    constructor(params: {
        schema: string,
        channelName: string,
        registerFn: BatchedCommandRegisterFn
    }) {
        this.schema = params.schema
        this.channelName = params.channelName
        this.registerFn = params.registerFn
    }

    create(params : {
        name?: string,
        lockMs?: number,
        content: Buffer,
        delayMs?: number,
    }) : Deferred<MessageCreateCommandResult> {
        const command = new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            name: params.name,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        })

        const deferred = new Deferred<MessageCreateCommandResult>()
        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                command.name,
                command.createdAt.toISOString(),
            ]),
            execute: (databaseClient) => command
                .execute(databaseClient)
                .then((x) => deferred.set(x))
        })

        return deferred
    }
}

import { MessageCreateCommand } from "@src/command/message-create"
import type { BatchedCommandRegisterFn } from "@src/queue/batch"

export type QueueBatchChannelMessageCreateResult = {
    messageId: string,
    promise: Promise<void>
}

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
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) : QueueBatchChannelMessageCreateResult {
        const command = new MessageCreateCommand({
            schema: this.schema,
            channelName: this.channelName,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        })

        const promise = new Promise<void>((resolve) => {
            this.registerFn({
                sortKey: JSON.stringify([
                    command.channelName,
                    command.createdAt.toISOString(),
                ]),
                execute: (databaseClient) => command
                    .execute(databaseClient)
                    .then(() => resolve())
            })
        })

        return {
            messageId: command.id,
            promise: promise,
        }
    }
}

import { MessageCreateCommand } from "@src/command/message-create"
import type { BatchedCommandRegisterFn } from "@src/queue/batch"

export class QueueBatchMessageModule {

    private readonly schema: string
    private readonly registerFn: BatchedCommandRegisterFn

    constructor(params: {
        schema: string,
        registerFn: BatchedCommandRegisterFn
    }) {
        this.schema = params.schema
        this.registerFn = params.registerFn
    }

    create(params : {
        lockMs: number,
        content: Buffer,
        delayMs?: number,
    }) {
        const command = new MessageCreateCommand({
            schema: this.schema,
            content: params.content,
            lockMs: params.lockMs,
            delayMs: params.delayMs,
        })

        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                command.createdAt.toISOString(),
            ]),
            execute: (db) => command.execute(db)
        })

        return {
            messageId: command.id,
            channelName: command.channelName
        }
    }
}

import { ChannelPolicyClearCommand } from "@src/command/channel-policy-clear"
import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import type { BatchedCommandRegisterFn } from "@src/queue/batch"

export class QueueBatchChannelPolicyModule {

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

    set(params : {
        maxConcurrency?: number | null,
        releaseIntervalMs?: number | null
    }) {
        const command = new ChannelPolicySetCommand({
            schema: this.schema,
            channelName: this.channelName,
            maxConcurrency: params.maxConcurrency,
            releaseIntervalMs: params.releaseIntervalMs
        })

        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                command.createdAt.toISOString(),
            ]),
            execute: async (databaseClient) => {
                await command.execute(databaseClient)
            }
        })
    }

    clear() {
        const command = new ChannelPolicyClearCommand({
            schema: this.schema,
            channelName: this.channelName
        })

        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                command.createdAt.toISOString(),
            ]),
            execute: async (databaseClient) => {
                await command.execute(databaseClient)
            }
        })
    }

}

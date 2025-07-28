import { ChannelPolicyClearCommand } from "@src/command/channel-policy-clear"
import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import { Deferred } from "@src/core/deferred"
import type { BatchedCommandRegisterFn } from "@src/queue/batch"

export class QueueBatchChannelPolicy {

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
        maxConcurrency: number | null,
        maxSize: number | null,
    }) : Deferred<void> {
        const command = new ChannelPolicySetCommand({
            schema: this.schema,
            channelName: this.channelName,
            maxConcurrency: params.maxConcurrency,
            maxSize: params.maxSize
        })

        const deferred = new Deferred<void>()
        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                null,
                command.createdAt.toISOString(),
            ]),
            execute: (databaseClient) => command
                .execute(databaseClient)
                .then((x) => deferred.set(x))
        })
        return deferred
    }

    clear() : Deferred<void> {
        const command = new ChannelPolicyClearCommand({
            schema: this.schema,
            channelName: this.channelName
        })

        const deferred = new Deferred<void>()
        this.registerFn({
            sortKey: JSON.stringify([
                command.channelName,
                null,
                command.createdAt.toISOString(),
            ]),
            execute: (databaseClient) => command
                .execute(databaseClient)
                .then((x) => deferred.set(x))
        })
        return deferred
    }

}

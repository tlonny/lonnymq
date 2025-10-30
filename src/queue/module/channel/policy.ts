import { ChannelPolicyClearCommand } from "@src/command/channel-policy-clear"
import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import type { DatabaseClientAdaptor } from "@src/core/database"

export class QueueChannelPolicyModule<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>
    private readonly channelId: string

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelId: string,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.channelId = params.channelId
    }

    set(params : {
        databaseClient: T,
        maxConcurrency?: number | null,
        maxSize?: number | null,
        releaseIntervalMs?: number | null
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new ChannelPolicySetCommand({
            schema: this.schema,
            channelId: this.channelId,
            maxConcurrency: params.maxConcurrency,
            maxSize: params.maxSize,
            releaseIntervalMs: params.releaseIntervalMs
        }).execute(adaptedClient)
    }

    clear(params: {
        databaseClient: T,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new ChannelPolicyClearCommand({
            schema: this.schema,
            channelId: this.channelId
        }).execute(adaptedClient)
    }

}

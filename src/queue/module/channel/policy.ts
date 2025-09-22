import { ChannelPolicyClearCommand } from "@src/command/channel-policy-clear"
import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import type { DatabaseClientAdaptor } from "@src/core/database"

export class QueueChannelPolicyModule<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>
    private readonly channelName: string

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelName: string,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.channelName = params.channelName
    }

    set(params : {
        databaseClient: T,
        maxConcurrency?: number | null,
        releaseIntervalMs?: number | null
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new ChannelPolicySetCommand({
            schema: this.schema,
            channelName: this.channelName,
            maxConcurrency: params.maxConcurrency,
            releaseIntervalMs: params.releaseIntervalMs
        }).execute(adaptedClient)
    }

    clear(params: {
        databaseClient: T,
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new ChannelPolicyClearCommand({
            schema: this.schema,
            channelName: this.channelName
        }).execute(adaptedClient)
    }

}

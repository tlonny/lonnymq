import { ChannelPolicyClearCommand } from "@src/command/channel-policy-clear"
import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import type { DatabaseClient } from "@src/core/database"

export class QueueChannelPolicy {

    private readonly schema: string
    private readonly channelName: string

    constructor(params: {
        schema: string,
        channelName: string,
    }) {
        this.schema = params.schema
        this.channelName = params.channelName
    }

    set(params : {
        databaseClient: DatabaseClient,
        maxConcurrency?: number | null,
        maxSize?: number | null,
        releaseIntervalMs?: number | null
    }) {
        return new ChannelPolicySetCommand({
            schema: this.schema,
            channelName: this.channelName,
            maxConcurrency: params.maxConcurrency,
            maxSize: params.maxSize,
            releaseIntervalMs: params.releaseIntervalMs
        }).execute(params.databaseClient)
    }

    clear(params: {
        databaseClient: DatabaseClient,
    }) {
        return new ChannelPolicyClearCommand({
            schema: this.schema,
            channelName: this.channelName
        }).execute(params.databaseClient)
    }

}

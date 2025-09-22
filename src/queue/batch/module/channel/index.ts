import type { BatchedCommandRegisterFn } from "@src/queue/batch"
import { QueueBatchChannelMessageModule } from "@src/queue/batch/module/channel/message"
import { QueueBatchChannelPolicyModule } from "@src/queue/batch/module/channel/policy"

export class QueueBatchChannelModule {

    readonly policy : QueueBatchChannelPolicyModule
    readonly message: QueueBatchChannelMessageModule

    constructor(params: {
        schema: string,
        registerFn: BatchedCommandRegisterFn
        channelName: string,
    }) {

        this.message = new QueueBatchChannelMessageModule(params)
        this.policy = new QueueBatchChannelPolicyModule(params)
    }

}

import type { BatchedCommandRegisterFn } from "@src/queue/batch"
import { QueueBatchChannelMessage } from "@src/queue/batch/channel/message"
import { QueueBatchChannelPolicy } from "@src/queue/batch/channel/policy"

export class QueueBatchChannel {

    readonly policy : QueueBatchChannelPolicy
    readonly message: QueueBatchChannelMessage

    constructor(params: {
        schema: string,
        registerFn: BatchedCommandRegisterFn
        channelName: string,
    }) {

        this.message = new QueueBatchChannelMessage(params)
        this.policy = new QueueBatchChannelPolicy(params)
    }

}

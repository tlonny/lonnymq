import { QueueChannelMessage } from "@src/queue/channel/message"
import { QueueChannelPolicy } from "@src/queue/channel/policy"

export class QueueChannel {

    readonly policy : QueueChannelPolicy
    readonly message: QueueChannelMessage

    constructor(params: {
        schema: string,
        channelName: string,
    }) {

        this.message = new QueueChannelMessage({
            schema: params.schema,
            channelName: params.channelName
        })

        this.policy = new QueueChannelPolicy({
            schema: params.schema,
            channelName: params.channelName
        })
    }

}

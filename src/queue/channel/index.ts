import type { DatabaseClientAdaptor } from "@src/core/database"
import { QueueChannelMessage } from "@src/queue/channel/message"
import { QueueChannelPolicy } from "@src/queue/channel/policy"

export class QueueChannel<T> {

    readonly policy : QueueChannelPolicy<T>
    readonly message: QueueChannelMessage<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelName: string,
    }) {

        this.message = new QueueChannelMessage({
            schema: params.schema,
            adaptor: params.adaptor,
            channelName: params.channelName
        })

        this.policy = new QueueChannelPolicy({
            schema: params.schema,
            adaptor: params.adaptor,
            channelName: params.channelName
        })
    }

}

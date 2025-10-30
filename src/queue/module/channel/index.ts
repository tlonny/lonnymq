import type { DatabaseClientAdaptor } from "@src/core/database"
import { QueueChannelMessageModule } from "@src/queue/module/channel/message"
import { QueueChannelPolicyModule } from "@src/queue/module/channel/policy"

export class QueueChannelModule<T> {

    readonly policy : QueueChannelPolicyModule<T>
    readonly message: QueueChannelMessageModule<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelId: string,
    }) {

        this.message = new QueueChannelMessageModule({
            schema: params.schema,
            adaptor: params.adaptor,
            channelId: params.channelId
        })

        this.policy = new QueueChannelPolicyModule({
            schema: params.schema,
            adaptor: params.adaptor,
            channelId: params.channelId
        })
    }

}

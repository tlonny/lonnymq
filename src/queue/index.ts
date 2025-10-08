import { MessageDequeueCommand } from "@src/command/message-dequeue"
import type { DatabaseClient, DatabaseClientAdaptor } from "@src/core/database"
import { dedent } from "@src/core/dedent"
import { installTableChannelPolicy } from "@src/install/00-table-channel-policy"
import { installTableChannelState } from "@src/install/01-table-channel-state"
import { installTableMessage } from "@src/install/02-table-message"
import { installFunctionMessageCreate } from "@src/install/03-function-message-create"
import { installFunctionMessageDequeue } from "@src/install/04-function-message-dequeue"
import { installFunctionMessageDelete } from "@src/install/05-function-message-delete"
import { installFunctionMessageDefer } from "@src/install/06-function-message-defer"
import { installFunctionChannelPolicyClear } from "@src/install/08-function-channel-policy-clear"
import { installFunctionChannelPolicySet } from "@src/install/09-function-channel-policy-set"
import { QueueChannelModule } from "@src/queue/module/channel"
import { QueueMessage } from "@src/queue/message"
import { QueueMessageModule } from "@src/queue/module/message"
import { installFunctionMessageHeartbeat } from "@src/install/07-function-message-heartbeat"
import { queueEventDecode } from "@src/queue/event"

type MessageDequeueResult<T> =
    | { resultType: "MESSAGE_NOT_AVAILABLE" }
    | { resultType: "MESSAGE_DEQUEUED", message: QueueMessage<T> }

type QueueParams<T> = T extends DatabaseClient
    ? { schema: string, adaptor?: DatabaseClientAdaptor<T> }
    : { schema: string, adaptor: DatabaseClientAdaptor<T> }

export class Queue<T = DatabaseClient> {

    readonly schema: string
    readonly message : QueueMessageModule<T>

    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params : QueueParams<T>) {
        this.schema = params.schema
        this.adaptor = params.adaptor
            ? params.adaptor
            : (x : DatabaseClient) => x
        this.message = new QueueMessageModule({
            schema: this.schema,
            adaptor: this.adaptor
        })
    }

    async dequeue(params: {
        databaseClient: T
        lockMs: number
    }): Promise<MessageDequeueResult<T>> {
        const command = new MessageDequeueCommand({
            schema: this.schema,
            lockMs: params.lockMs,
        })

        const adaptedClient = this.adaptor(params.databaseClient)
        const result = await command.execute(adaptedClient)

        if (result.resultType === "MESSAGE_DEQUEUED") {
            return {
                resultType: "MESSAGE_DEQUEUED",
                message: new QueueMessage({
                    schema: this.schema,
                    adaptor: this.adaptor,
                    id: result.id,
                    channelName: result.channelName,
                    isUnlocked: result.isUnlocked,
                    content: result.content,
                    state: result.state,
                    numAttempts: result.numAttempts,
                })
            }
        } else {
            return result
        }
    }

    channel(channelName: string) {
        return new QueueChannelModule({
            adaptor: this.adaptor,
            schema: this.schema,
            channelName: channelName
        })
    }

    install(params: {
        eventChannel?: string,
    } = {}) : string[] {
        return [
            installTableChannelPolicy,
            installTableChannelState,
            installTableMessage,
            installFunctionMessageCreate,
            installFunctionMessageDequeue,
            installFunctionMessageDelete,
            installFunctionMessageDefer,
            installFunctionMessageHeartbeat,
            installFunctionChannelPolicySet,
            installFunctionChannelPolicyClear,
        ]
            .sort((a, b) => a.name.localeCompare(b.name))
            .flatMap(install => install.sql({
                schema: this.schema,
                eventChannel: params.eventChannel ?? null,
            })).map(sql => dedent(sql.value))
    }

    static decode(payload : string) {
        return queueEventDecode(payload)
    }
}

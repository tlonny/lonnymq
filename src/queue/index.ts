import { MessageDequeueCommand } from "@src/command/message-dequeue"
import type { DatabaseClient, DatabaseClientAdaptor } from "@src/core/database"
import { dedent } from "@src/core/dedent"
import { migrationTableChannelPolicy } from "@src/migration/00-table-channel-policy"
import { migrationTableChannelState } from "@src/migration/01-table-channel-state"
import { migrationTableMessage } from "@src/migration/02-table-message"
import { migrationFunctionMessageCreate } from "@src/migration/03-function-message-create"
import { migrationFunctionMessageDequeue } from "@src/migration/04-function-message-dequeue"
import { migrationFunctionMessageDelete } from "@src/migration/05-function-message-delete"
import { migrationFunctionMessageDefer } from "@src/migration/06-function-message-defer"
import { migrationFunctionMessageHeartbeat } from "@src/migration/07-function-message-heartbeat"
import { migrationFunctionChannelPolicyClear } from "@src/migration/08-function-channel-policy-clear"
import { migrationFunctionChannelPolicySet } from "@src/migration/09-function-channel-policy-set"
import { QueueBatch } from "@src/queue/batch"
import { QueueChannelModule } from "@src/queue/module/channel"
import { QueueMessage } from "@src/queue/message"
import { QueueMessageModule } from "@src/queue/module/message"

export type MessageDequeueResult<T> =
    | { resultType: "MESSAGE_NOT_AVAILABLE", retryMs: number | null }
    | { resultType: "MESSAGE_DEQUEUED", message: QueueMessage<T> }

type QueueParams<T> = T extends DatabaseClient
    ? { schema: string, adaptor?: DatabaseClientAdaptor<T> }
    : { schema: string, adaptor: DatabaseClientAdaptor<T> }

export class Queue<T = DatabaseClient> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>
    readonly message : QueueMessageModule<T>

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
    }): Promise<MessageDequeueResult<T>> {
        const command = new MessageDequeueCommand({ schema: this.schema })
        const adaptedClient = this.adaptor(params.databaseClient)
        const result = await command.execute(adaptedClient)

        if (result.resultType === "MESSAGE_DEQUEUED") {
            return {
                resultType: "MESSAGE_DEQUEUED",
                message: new QueueMessage({
                    schema: this.schema,
                    adaptor: this.adaptor,
                    id: result.message.id,
                    channelName: result.message.channelName,
                    isUnlocked: result.message.isUnlocked,
                    content: result.message.content,
                    state: result.message.state,
                    numAttempts: result.message.numAttempts,
                })
            }
        } else {
            return result
        }
    }

    channel(channelName: string): QueueChannelModule<T> {
        return new QueueChannelModule({
            adaptor: this.adaptor,
            schema: this.schema,
            channelName: channelName
        })
    }

    batch() {
        return new QueueBatch<T>({
            schema: this.schema,
            adaptor: this.adaptor
        })
    }

    migrations(params: {
        eventChannel?: string,
    } = {}) : string[] {
        return [
            migrationTableChannelPolicy,
            migrationTableChannelState,
            migrationTableMessage,
            migrationFunctionMessageCreate,
            migrationFunctionMessageDequeue,
            migrationFunctionMessageDelete,
            migrationFunctionMessageDefer,
            migrationFunctionMessageHeartbeat,
            migrationFunctionChannelPolicySet,
            migrationFunctionChannelPolicyClear,
        ]
            .sort((a, b) => a.name.localeCompare(b.name))
            .flatMap(migration => migration.sql({
                schema: this.schema,
                eventChannel: params.eventChannel ?? null,
            })).map(sql => dedent(sql.value))
    }
}

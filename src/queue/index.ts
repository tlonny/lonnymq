import { MessageDequeueCommand } from "@src/command/message-dequeue"
import type { DatabaseClient } from "@src/core/database"
import { dedent } from "@src/core/dedent"
import { migrationTableChannelPolicy } from "@src/migration/00-table-channel-policy"
import { migrationTableChannelState } from "@src/migration/01-table-channel-state"
import { migrationTableMessage } from "@src/migration/02-table-message"
import { migrationFunctionMessageCreate } from "@src/migration/03-function-message-create"
import { migrationFunctionMessageDequeue } from "@src/migration/04-function-message-dequeue"
import { migrationFunctionMessageDelete } from "@src/migration/05-function-message-delete"
import { migrationFunctionMessageDefer } from "@src/migration/06-function-message-defer"
import { migrationFunctionChannelPolicySet } from "@src/migration/07-function-channel-policy-set"
import { migrationFunctionChannelPolicyClear } from "@src/migration/08-function-channel-policy-clear"
import { QueueBatch } from "@src/queue/batch"
import { QueueChannel } from "@src/queue/channel"
import { QueueMessage } from "@src/queue/message"

export type MessageDequeueResult =
    | { resultType: "MESSAGE_NOT_AVAILABLE", retryMs: number | null }
    | { resultType: "MESSAGE_DEQUEUED", message: QueueMessage }

export class Queue {
    private readonly schema: string

    constructor(params : { schema: string }) {
        this.schema = params.schema
    }

    async dequeue(params: {
        databaseClient: DatabaseClient
    }): Promise<MessageDequeueResult> {
        const command = new MessageDequeueCommand({ schema: this.schema })
        const result = await command.execute(params.databaseClient)

        if (result.resultType === "MESSAGE_DEQUEUED") {
            return {
                resultType: "MESSAGE_DEQUEUED",
                message: new QueueMessage({
                    schema: this.schema,
                    id: result.message.id,
                    channelName: result.message.channelName,
                    name: result.message.name,
                    content: result.message.content,
                    state: result.message.state,
                    numAttempts: result.message.numAttempts,
                })
            }
        } else {
            return result
        }
    }

    channel(channelName: string): QueueChannel {
        return new QueueChannel({
            schema: this.schema,
            channelName: channelName
        })
    }

    batch() {
        return new QueueBatch({ schema: this.schema })
    }

    migrations(params: {
        eventChannel?: string,
    }) : string[] {
        return [
            migrationTableChannelPolicy,
            migrationTableChannelState,
            migrationTableMessage,
            migrationFunctionMessageCreate,
            migrationFunctionMessageDequeue,
            migrationFunctionMessageDelete,
            migrationFunctionMessageDefer,
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

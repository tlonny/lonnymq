import { MessageDeferCommand, type MessageDeferCommandResult } from "@src/command/message-defer"
import { MessageDeleteCommand, type MessageDeleteCommandResult } from "@src/command/message-delete"
import type { DatabaseClient } from "@src/core/database"


export class QueueMessage {

    private readonly schema: string

    readonly id : string
    readonly channelName: string
    readonly name: string | null
    readonly content: string
    readonly dequeueId: string
    readonly state: string | null
    readonly numAttempts: number
    readonly lockMs: number

    constructor(params: {
        schema: string,
        id: string,
        dequeueId: string,
        channelName: string,
        name: string | null,
        content: string,
        state: string | null,
        numAttempts: number,
        lockMs: number
    }) {
        this.schema = params.schema
        this.id = params.id
        this.channelName = params.channelName
        this.dequeueId = params.dequeueId
        this.name = params.name
        this.content = params.content
        this.state = params.state
        this.numAttempts = params.numAttempts
        this.lockMs = params.lockMs
    }

    async defer(params: {
        databaseClient: DatabaseClient,
        delayMs?: number,
        priority?: boolean,
        state?: string
    }) : Promise<MessageDeferCommandResult> {
        return new MessageDeferCommand({
            schema: this.schema,
            id: this.id,
            dequeueId: this.dequeueId,
            delayMs: params.delayMs,
            state: params.state,
            priority: params.priority
        }).execute(params.databaseClient)
    }

    async delete(params: {
        databaseClient: DatabaseClient,
    }) : Promise<MessageDeleteCommandResult> {
        return new MessageDeleteCommand({
            schema: this.schema,
            id: this.id,
            dequeueId: this.dequeueId
        }).execute(params.databaseClient)
    }

}

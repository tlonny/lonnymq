import { MessageDeferCommand, type MessageDeferCommandResult } from "@src/command/message-defer"
import { MessageDeleteCommand, type MessageDeleteCommandResult } from "@src/command/message-delete"
import type { DatabaseClient } from "@src/core/database"


export class QueueMessage {

    private readonly schema: string

    readonly id : bigint
    readonly channelName: string
    readonly name: string | null
    readonly content: Buffer
    readonly dequeueNonce: string
    readonly state: Buffer | null
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        id: bigint,
        dequeueNonce: string,
        channelName: string,
        name: string | null,
        content: Buffer,
        state: Buffer | null,
        numAttempts: number,
    }) {
        this.schema = params.schema
        this.id = params.id
        this.channelName = params.channelName
        this.dequeueNonce = params.dequeueNonce
        this.name = params.name
        this.content = params.content
        this.state = params.state
        this.numAttempts = params.numAttempts
    }

    async defer(params: {
        databaseClient: DatabaseClient,
        delayMs?: number,
        state?: Buffer
    }) : Promise<MessageDeferCommandResult> {
        return new MessageDeferCommand({
            schema: this.schema,
            id: this.id,
            dequeueNonce: this.dequeueNonce,
            delayMs: params.delayMs,
            state: params.state,
        }).execute(params.databaseClient)
    }

    async delete(params: {
        databaseClient: DatabaseClient,
    }) : Promise<MessageDeleteCommandResult> {
        return new MessageDeleteCommand({
            schema: this.schema,
            id: this.id,
            dequeueNonce: this.dequeueNonce
        }).execute(params.databaseClient)
    }

}

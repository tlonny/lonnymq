import { MessageDeferCommand, type MessageDeferCommandResult } from "@src/command/message-defer"
import { MessageDeleteCommand, type MessageDeleteCommandResult } from "@src/command/message-delete"
import { MessageHeartbeatCommand, type MessageHeartbeatCommandResult } from "@src/command/message-heartbeat"
import type { DatabaseClientAdaptor } from "@src/core/database"

export class QueueMessage<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    readonly id : string
    readonly isUnlocked: boolean
    readonly channelName: string
    readonly content: Buffer
    readonly state: Buffer | null
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        id: string,
        channelName: string,
        isUnlocked: boolean,
        content: Buffer,
        state: Buffer | null,
        numAttempts: number,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.id = params.id
        this.channelName = params.channelName
        this.isUnlocked = params.isUnlocked
        this.content = params.content
        this.state = params.state
        this.numAttempts = params.numAttempts
    }

    async defer(params: {
        databaseClient: T,
        delayMs?: number,
        state?: Buffer
    }) : Promise<MessageDeferCommandResult> {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageDeferCommand({
            schema: this.schema,
            id: this.id,
            numAttempts: this.numAttempts,
            delayMs: params.delayMs,
            state: params.state,
        }).execute(adaptedClient)
    }

    async delete(params: {
        databaseClient: T,
    }) : Promise<MessageDeleteCommandResult> {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageDeleteCommand({
            schema: this.schema,
            numAttempts: this.numAttempts,
            id: this.id,
        }).execute(adaptedClient)
    }

    async heartbeat(params: {
        databaseClient: T,
        lockMs: number
    }) : Promise<MessageHeartbeatCommandResult> {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageHeartbeatCommand({
            schema: this.schema,
            id: this.id,
            numAttempts: this.numAttempts,
            lockMs: params.lockMs,
        }).execute(adaptedClient)
    }
}

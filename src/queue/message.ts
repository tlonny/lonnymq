import { MessageDeferCommand } from "@src/command/message-defer"
import { MessageDeleteCommand } from "@src/command/message-delete"
import { MessageHeartbeatCommand } from "@src/command/message-heartbeat"
import type { DatabaseClientAdaptor } from "@src/core/database"

export class QueueMessage<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    readonly id : bigint
    readonly isUnlocked: boolean
    readonly channelId: string
    readonly content: Buffer
    readonly state: Buffer | null
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        id: bigint,
        channelId: string,
        isUnlocked: boolean,
        content: Buffer,
        state: Buffer | null,
        numAttempts: number,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.id = params.id
        this.channelId = params.channelId
        this.isUnlocked = params.isUnlocked
        this.content = params.content
        this.state = params.state
        this.numAttempts = params.numAttempts
    }

    async defer(params: {
        databaseClient: T,
        state? : Buffer,
        dequeueAt?: number
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        return new MessageDeferCommand({
            schema: this.schema,
            id: this.id,
            numAttempts: this.numAttempts,
            dequeueAt: params.dequeueAt ?? null,
            state: params.state ?? null,
        }).execute(adaptedClient)
    }

    async delete(params: {
        databaseClient: T,
    }) {
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
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)
        return new MessageHeartbeatCommand({
            schema: this.schema,
            id: this.id,
            numAttempts: this.numAttempts,
            lockMs: params.lockMs,
        }).execute(adaptedClient)
    }
}

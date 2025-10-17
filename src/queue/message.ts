import { MessageDeferCommand } from "@src/command/message-defer"
import { MessageDeleteCommand } from "@src/command/message-delete"
import { MessageHeartbeatCommand } from "@src/command/message-heartbeat"
import type { DatabaseClientAdaptor } from "@src/core/database"

export type QueueMessageScheduleParams =
    | { scheduleType: "OFFSET", offsetMs: number }
    | { scheduleType: "TIMESTAMP", timestamp: number }

export class QueueMessage<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    readonly id : bigint
    readonly isUnlocked: boolean
    readonly channelName: string
    readonly content: Buffer
    readonly state: Buffer | null
    readonly numAttempts: number

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        id: bigint,
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
        state? : Buffer,
        schedule?: QueueMessageScheduleParams
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const offsetMs = params.schedule && params.schedule.scheduleType === "OFFSET"
            ? params.schedule.offsetMs
            : null

        const timestamp = params.schedule && params.schedule.scheduleType === "TIMESTAMP"
            ? params.schedule.timestamp
            : null

        return new MessageDeferCommand({
            schema: this.schema,
            id: this.id,
            numAttempts: this.numAttempts,
            offsetMs: offsetMs,
            timestamp: timestamp,
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

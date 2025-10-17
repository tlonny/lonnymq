import { randomUUID } from "crypto"
import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

export type QueueMessageModuleScheduleParams =
    | { scheduleType: "OFFSET", offsetMs: number }
    | { scheduleType: "TIMESTAMP", timestamp: number }

export class QueueMessageModule<T> {

    private readonly schema: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
    }

    async create(params : {
        databaseClient: T,
        content: Buffer,
        schedule?: QueueMessageModuleScheduleParams
    }) {
        const adaptedClient = this.adaptor(params.databaseClient)

        const offsetMs = params.schedule && params.schedule.scheduleType === "OFFSET"
            ? params.schedule.offsetMs
            : null

        const timestamp = params.schedule && params.schedule.scheduleType === "TIMESTAMP"
            ? params.schedule.timestamp
            : null

        const command = new MessageCreateCommand({
            schema: this.schema,
            content: params.content,
            offsetMs: offsetMs,
            timestamp: timestamp,
            channelName: randomUUID(),
        })

        const result = await command.execute(adaptedClient)
        return {
            messageId: result.id,
            channelName: command.channelName,
            channelSize: result.channelSize,
        }
    }
}


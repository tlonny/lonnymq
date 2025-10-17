import type { DatabaseClientAdaptor } from "@src/core/database"
import { MessageCreateCommand } from "@src/command/message-create"

export type QueueChannelMessageModuleScheduleParams =
    | { scheduleType: "OFFSET", offsetMs: number }
    | { scheduleType: "TIMESTAMP", timestamp: number }

export class QueueChannelMessageModule<T> {

    private readonly schema: string
    private readonly channelName: string
    private readonly adaptor: DatabaseClientAdaptor<T>

    constructor(params: {
        schema: string,
        adaptor: DatabaseClientAdaptor<T>
        channelName: string,
    }) {
        this.schema = params.schema
        this.adaptor = params.adaptor
        this.channelName = params.channelName
    }

    async create(params : {
        databaseClient: T,
        content: Buffer,
        schedule?: QueueChannelMessageModuleScheduleParams
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
            channelName: this.channelName,
            content: params.content,
            offsetMs: offsetMs,
            timestamp: timestamp,
        })

        const result = await command.execute(adaptedClient)
        return {
            messageId: result.id,
            channelName: command.channelName,
            channelSize: result.channelSize
        }
    }
}

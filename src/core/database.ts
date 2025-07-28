type DatabaseCommand = {
    sortKey: string,
    command: (databaseClient: DatabaseClient) => Promise<void>
}

export type DatabaseClientQueryResult = {
    rows: Array<Record<string, unknown>>
}

export interface DatabaseClient {
    query(query : string) : Promise<DatabaseClientQueryResult>
}

export class DatabaseCommandBatcher {

    private readonly commands: DatabaseCommand[]

    constructor() {
        this.commands = []
    }

    addCommand(command: DatabaseCommand) {
        this.commands.push(command)
    }

    async execute(databaseClient: DatabaseClient): Promise<void> {
        this.commands.sort((a, b) => a.sortKey.localeCompare(b.sortKey))
        for (const command of this.commands) {
            await command.command(databaseClient)
        }
    }

}

export type DatabaseClientQueryResult = {
    rows: Array<Record<string, unknown>>
}

export interface DatabaseClient {
    query(query : string, params: Array<unknown>): Promise<DatabaseClientQueryResult>
}

export type DatabaseClientAdaptor<T> = (client: T) => DatabaseClient

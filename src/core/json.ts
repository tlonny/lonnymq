export type JsonParseResult =
    | { resultType: "PARSE_SUCCESS", data: unknown }
    | { resultType: "PARSE_FAILURE", error: Error }

export const jsonParse = (input : string) : JsonParseResult => {
    try {
        const parsed = JSON.parse(input)
        return { resultType: "PARSE_SUCCESS", data: parsed }
    } catch (error) {
        return { resultType: "PARSE_FAILURE", error: error as Error }
    }
}

type SqlValue = null | number | string | boolean | Date


export type SqlValueNode<T extends SqlValue> = { nodeType: "VALUE", value: T }
export type SqlRefNode = { nodeType: "REF", value: string }
export type SqlRawNode = { nodeType: "RAW", value: string }

export type SqlNode =
    | SqlValueNode<any>
    | SqlRefNode
    | SqlRawNode

export const value = <T extends SqlValue>(value: T): SqlValueNode<T> => ({
    nodeType: "VALUE",
    value
})

export const ref = (value: string): SqlRefNode => ({
    nodeType: "REF",
    value: value
})

export const raw = (value: string): SqlRawNode => ({
    nodeType: "RAW",
    value
})

export const stringEscape = (value : string): string => {
    const escaped = value.replace(/'/g, "''")
    return `'${escaped}'`
}

export const valueEscape = (value: SqlValue): string => {
    if (value === null) {
        return "NULL"
    } else if (typeof value === "string") {
        return stringEscape(value)
    } else if (typeof value === "number") {
        return value.toString()
    } else if (typeof value === "boolean") {
        return value ? "TRUE" : "FALSE"
    } else if (value instanceof Date) {
        return `'${value.toISOString()}'`
    } else {
        value satisfies never
        throw new Error(`Unsupported value type: ${typeof value}`)
    }
}

export const refEscape = (value: string): string => {
    const escaped = value.replace(/"/g, "\"\"")
    return `"${escaped}"`
}

export const escape = (value: SqlNode): string => {
    if (value.nodeType === "VALUE") {
        return valueEscape(value.value)
    } else if (value.nodeType === "REF") {
        return refEscape(value.value)
    } else if (value.nodeType === "RAW") {
        return value.value
    } else {
        value satisfies never
        throw new Error("Unsupported SQL node type")
    }
}

export const objectBuild = (obj: Record<string, SqlNode>): SqlNode => {
    const parts: string[] = []
    for (const [key, value] of Object.entries(obj)) {
        parts.push(stringEscape(key))
        parts.push(escape(value))
    }

    return raw(`JSONB_BUILD_OBJECT(${parts.join(",")})`)
}

export const sql = (fragments: TemplateStringsArray, ...values: SqlNode[]): SqlRawNode => {
    const parts: string[] = []

    for (let ix = 0; ix < fragments.length; ix += 1) {
        parts.push(fragments[ix])

        if (ix < values.length) {
            parts.push(escape(values[ix]))
        }
    }

    return raw(parts.join(""))
}

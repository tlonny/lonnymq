export const dedent = (text : string): string => {
    const lines = text.split("\n")
    let minIndent = Number.MAX_SAFE_INTEGER

    for (const line of lines) {
        if (line.trim().length === 0) {
            continue
        }

        const indent = line.search(/\S/)
        minIndent = Math.min(minIndent, indent)
    }

    return lines
        .map(line => line.slice(minIndent))
        .join("\n")
        .trim()
}

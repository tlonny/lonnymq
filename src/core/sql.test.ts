import { raw, ref, sql, value, type SqlNode } from "@src/core/sql"
import { describe, expect, it } from "bun:test"

describe("sql", () => {

    const testCases : [SqlNode, string ][] = [
        [raw("\"FOO\""), "\"FOO\""],
        [ref("FOO"), "\"FOO\""],
        [ref("'FOO'"), "\"'FOO'\""],
        [ref("\"FOO\""), "\"\"\"FOO\"\"\""],
        [value(123), "123"],
        [value("123"), "'123'"],
        [value(null), "NULL"],
    ]

    for (const [input, expected] of testCases) {
        it(`${input.nodeType}:${input.value} is expected to be ${expected}`, () => {
            expect(sql`${input}`.value).toBe(expected)
        })
    }

})

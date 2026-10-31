import { epoch } from "@src/install/function/epoch"
import { afterAll, beforeAll, expect, test } from "bun:test"
import { Client } from "pg"

let client: Client

beforeAll(async () => {
    client = new Client({ connectionString: process.env.DATABASE_URL })
    await client.connect()
})

afterAll(async () => {
    await client.end()
})

test.each([
    ["2026-10-31T12:00:00.000000Z", 1793448000000],
    ["2026-10-31T12:00:00.499999Z", 1793448000499],
    ["2026-10-31T12:00:00.500001Z", 1793448000500],
    ["2026-10-31T12:00:30.750000Z", 1793448030750],
    ["2026-10-31T12:00:59.999999Z", 1793448059999],
    ["2026-10-31T12:01:00.000000Z", 1793448060000],
    ["2026-10-31T13:00:30.750000+01:00", 1793448030750],
    ["1969-12-31T23:59:59.999999Z", -1],
])("epoch converts %s to Unix milliseconds", async (timestamp, expected) => {
    await client.query("DROP FUNCTION IF EXISTS pg_temp.epoch()")
    const [sql] = epoch({ schema: "pg_temp", eventChannel: null })
    // Freeze the clock in the generated function for deterministic boundary checks.
    expect(sql.value).toContain("NOW()")
    await client.query(sql.value.replace("NOW()", `TIMESTAMPTZ '${timestamp}'`))
    const result = await client.query("SELECT pg_temp.epoch() AS value")
    expect(Number(result.rows[0].value)).toBe(expected)
})

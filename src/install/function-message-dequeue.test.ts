import { sql, value } from "@src/core/sql"
import { channelDequeueQuery, messageLockedDequeueQuery, messageNextDequeueQuery } from "@src/install/function-message-dequeue"
import { Queue } from "@src/queue"
import { beforeEach, test, expect } from "bun:test"
import { Pool } from "pg"

const SCHEMA = "test"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: SCHEMA })

const INDEX_SCAN_REGEX = /(Index Scan|Index Only Scan)/

beforeEach(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`)
    await pool.query(`CREATE SCHEMA "${SCHEMA}"`)
    for (const install of queue.install()) {
        await pool.query(install)
    }
})

test("messageLockedDequeueQuery uses index scans", async () => {
    const client = await pool.connect()
    try {
        await client.query("BEGIN")
        await client.query("SET LOCAL enable_seqscan = OFF")
        await client.query("SET LOCAL enable_bitmapscan = OFF")
        const query = sql`
            EXPLAIN (COSTS OFF)
            ${messageLockedDequeueQuery({ now: sql`0`, schema: SCHEMA })}
        `
        const result = await client.query(query.value)
        expect(result.rows.length).toBeGreaterThan(0)
        expect(result.rows[0]["QUERY PLAN"]).toMatch(INDEX_SCAN_REGEX)
        await client.query("COMMIT")
    } finally {
        client.query("ROLLBACK")
        await client.release()
    }
})

test("channelDequeueQuery uses index scans", async () => {
    const client = await pool.connect()
    try {
        await client.query("BEGIN")
        await client.query("SET LOCAL enable_seqscan = OFF")
        await client.query("SET LOCAL enable_bitmapscan = OFF")
        const query = sql`
            EXPLAIN (COSTS OFF)
            ${channelDequeueQuery({ schema: SCHEMA })}
        `
        const result = await client.query(query.value)
        expect(result.rows.length).toBeGreaterThan(0)
        expect(result.rows[0]["QUERY PLAN"]).toMatch(INDEX_SCAN_REGEX)
        await client.query("COMMIT")
    } finally {
        client.query("ROLLBACK")
        await client.release()
    }
})

test("messageNextDequeueQuery uses index scans", async () => {
    const client = await pool.connect()
    try {
        await client.query("BEGIN")
        await client.query("SET LOCAL enable_seqscan = OFF")
        await client.query("SET LOCAL enable_bitmapscan = OFF")
        const query = sql`
            EXPLAIN (COSTS OFF)
            ${messageNextDequeueQuery({ schema: SCHEMA, channelId: value("foo") })}
        `
        const result = await client.query(query.value)
        expect(result.rows.length).toBeGreaterThan(0)
        expect(result.rows[0]["QUERY PLAN"]).toMatch(INDEX_SCAN_REGEX)
        await client.query("COMMIT")
    } finally {
        client.query("ROLLBACK")
        await client.release()
    }
})

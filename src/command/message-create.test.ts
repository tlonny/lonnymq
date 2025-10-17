import { MessageCreateCommand } from "@src/command/message-create"
import { ref, sql } from "@src/core/sql"
import { Queue } from "@src/queue"
import { queueEventDecode } from "@src/queue/event"
import { beforeEach, expect, test } from "bun:test"
import { Pool } from "pg"

const EVENT_CHANNEL = "events"
const SCHEMA = "test"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: SCHEMA })

beforeEach(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`)
    await pool.query(`CREATE SCHEMA "${SCHEMA}"`)
    for (const install of queue.install({ eventChannel: EVENT_CHANNEL })) {
        await pool.query(install)
    }
})

test("MessageCreateCommand persists a message in the DB", async () => {
    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello"),
        timestamp: null,
        offsetMs: 10,
    })

    const client = await pool.connect()
    await client.query(`LISTEN "${EVENT_CHANNEL}"`)
    let events: any[] = []
    client.on("notification", (msg) => {
        if (msg.channel === EVENT_CHANNEL) {
            events.push(queueEventDecode(msg.payload as string))
        }
    })

    try {
        const result = await command.execute(pool)
        const message = await pool.query("SELECT * FROM test.message").then(res => res.rows[0])
        const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])

        expect(message).toMatchObject({
            id: result.id.toString(),
            num_attempts: "0",
            content: Buffer.from("hello"),
            channel_name: "alpha",
        })

        expect(channelState).toMatchObject({
            message_id: result.id.toString(),
            current_size: 1,
            message_dequeue_at: message.dequeue_at
        })

        expect(events).toHaveLength(1)
        expect(events[0]).toMatchObject({
            eventType: "MESSAGE_CREATED",
            offsetMs: 10,
        })

    } finally {
        client.release()
    }
})

test("MessageCreateCommand correctly updates channelState when preempting a \"lower\" priority message", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        timestamp: null,
        offsetMs: 0,
        content: Buffer.from("hello"),
    })

    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        timestamp: null,
        offsetMs: -50,
        content: Buffer.from("hello"),
    })

    const firstResult = await firstCommand.execute(pool)
    const secondResult = await secondCommand.execute(pool)

    const firstMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [firstResult.id]).then(res => res.rows[0])
    const secondMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [secondResult.id]).then(res => res.rows[0])

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 2,
        dequeue_next_at: firstMessage.dequeue_at,
        message_dequeue_at: secondMessage.dequeue_at,
        message_id: secondResult.id.toString(),
    })
})

test("MessageCreateCommand correctly returns the channel size", async () => {
    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        timestamp: null,
        content: Buffer.from("hello"),
        offsetMs: 10,
    })

    const firstResult = await command.execute(pool)
    expect(firstResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        channelSize: 1
    })

    const secondResult = await command.execute(pool)
    expect(secondResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        channelSize: 2
    })

    const thirdResult = await command.execute(pool)
    expect(thirdResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        channelSize: 3
    })
})

test("MessageCreateCommand persists supplied dequeueAt", async () => {
    const dequeueAt = Date.now() + 5_321

    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello"),
        timestamp: dequeueAt,
        offsetMs: null,
    })

    const result = await command.execute(pool)
    const message = await pool
        .query("SELECT dequeue_at FROM test.message WHERE id = $1", [result.id])
        .then(res => res.rows[0])

    expect(Number(message.dequeue_at)).toBe(dequeueAt)
})

test("MessageCreateCommand sets dequeue_at = DB NOW() + offsetMs", async () => {
    const offsetMs = 1_234
    const client = await pool.connect()

    try {
        await client.query("BEGIN")

        const dbNow = await client
            .query(sql`SELECT ${ref(SCHEMA)}."epoch"() AS now`.value)
            .then(res => Number(res.rows[0].now))

        const command = new MessageCreateCommand({
            schema: SCHEMA,
            channelName: "alpha",
            content: Buffer.from("hello"),
            offsetMs,
            timestamp: null,
        })

        const result = await command.execute(client)
        const message = await client
            .query("SELECT dequeue_at FROM test.message WHERE id = $1", [result.id])
            .then(res => res.rows[0])

        expect(Number(message.dequeue_at)).toBe(dbNow + offsetMs)
        await client.query("ROLLBACK")
    } catch (e) {
        await client.query("ROLLBACK")
        throw e
    } finally {
        client.release()
    }
})

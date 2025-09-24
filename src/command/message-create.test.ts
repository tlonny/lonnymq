import { MessageCreateCommand } from "@src/command/message-create"
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
        delayMs: 10,
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
            id: result.metadata.id,
            num_attempts: "0",
            content: Buffer.from("hello"),
            channel_name: "alpha",
        })

        expect(channelState).toMatchObject({
            current_size: 1,
            message_dequeue_at: message.dequeue_at,
            message_id: result.metadata.id,
        })

        expect(events).toHaveLength(1)
        expect(events[0]).toMatchObject({
            eventType: "MESSAGE_CREATED",
            delayMs: 10,
        })

    } finally {
        client.release()
    }
})

test("MessageCreateCommand correctly updates channelState when preempting a \"lower\" priority message", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 0,
        content: Buffer.from("hello"),
    })

    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: -50,
        content: Buffer.from("hello"),
    })

    const firstResult = await firstCommand.execute(pool)
    const secondResult = await secondCommand.execute(pool)

    const firstMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [firstResult.metadata.id]).then(res => res.rows[0])
    const secondMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [secondResult.metadata.id]).then(res => res.rows[0])

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 2,
        active_next_at: firstMessage.dequeue_at,
        message_dequeue_at: secondMessage.dequeue_at,
        message_seq_no: secondMessage.seq_no,
        message_id: secondResult.metadata.id,
    })
})

test("MessageCreateCommand correctly returns the channel size", async () => {
    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello"),
        delayMs: 10,
    })

    const firstResult = await command.execute(pool)
    expect(firstResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        metadata: {
            channelSize: 1
        }
    })

    const secondResult = await command.execute(pool)
    expect(secondResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        metadata: {
            channelSize: 2
        }
    })

    const thirdResult = await command.execute(pool)
    expect(thirdResult).toMatchObject({
        resultType: "MESSAGE_CREATED",
        metadata: {
            channelSize: 3
        }
    })
})

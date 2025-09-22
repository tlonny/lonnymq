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
    for (const migration of queue.migrations({ eventChannel: EVENT_CHANNEL })) {
        await pool.query(migration)
    }
})

test("MessageCreateCommand persists a message in the DB", async () => {
    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello"),
        delayMs: 10,
        lockMs: 20,
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
        await command.execute(pool)
        const message = await pool.query("SELECT * FROM test.message").then(res => res.rows[0])
        const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])

        expect(message).toMatchObject({
            id: command.id,
            num_attempts: "0",
            content: Buffer.from("hello"),
            channel_name: "alpha",
        })

        expect(channelState).toMatchObject({
            current_size: 1,
            message_id: command.id,
            message_dequeue_at: message.dequeue_at,
        })

        expect(events).toHaveLength(1)
        expect(events[0]).toMatchObject({
            eventType: "MESSAGE_CREATED",
            id: command.id,
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
        lockMs: 600,
        content: Buffer.from("hello"),
    })

    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 600,
        delayMs: -50,
        content: Buffer.from("hello"),
    })

    await firstCommand.execute(pool)
    await secondCommand.execute(pool)

    const firstMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [firstCommand.id]).then(res => res.rows[0])
    const secondMessage = await pool.query("SELECT * FROM test.message WHERE id = $1", [secondCommand.id]).then(res => res.rows[0])

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 2,
        active_next_at: firstMessage.dequeue_at,
        message_dequeue_at: secondMessage.dequeue_at,
        message_seq_no: secondMessage.seq_no,
        message_id: secondCommand.id,
    })
})

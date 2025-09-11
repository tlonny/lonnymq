import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import { MessageCreateCommand } from "@src/command/message-create"
import { MessageDequeueCommand } from "@src/command/message-dequeue"
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
        name: "my-message",
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
        expect(result).toMatchObject({ resultType: "MESSAGE_CREATED" })

        const message = await pool.query("SELECT * FROM test.message").then(res => res.rows[0])
        const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])

        expect(message).toMatchObject({
            id: command.id,
            num_attempts: 0,
            content: Buffer.from("hello"),
            channel_name: "alpha",
        })

        expect(channelState).toMatchObject({
            name: "alpha",
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

test("MessageCreateCommand drops messages if size constaints are breached", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 20,
        content: Buffer.from("hello")
    })
    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 10,
        content: Buffer.from("world")
    })

    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        maxSize: 1,
    }).execute(pool)

    const firstResult = await firstCommand.execute(pool)
    expect(firstResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const secondResult = await secondCommand.execute(pool)
    expect(secondResult).toMatchObject({ resultType: "MESSAGE_DROPPED" })

    const messages = await pool.query("SELECT * FROM test.message").then(res => res.rows)
    expect(messages).toHaveLength(1)
    expect(messages[0]).toMatchObject({
        id: firstCommand.id,
        num_attempts: 0,
        content: Buffer.from("hello"),
        channel_name: "alpha",
    })

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 1,
        max_size: 1,
        message_id: firstCommand.id,
        message_dequeue_at: messages[0].dequeue_at,
    })
})

test("MessageCreateCommand deduplicates messages with the same name if not processed", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 20,
        content: Buffer.from("hello"),
        name: "my-message",
    })
    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 10,
        content: Buffer.from("world"),
        name: "my-message",
    })

    const firstResult = await firstCommand.execute(pool)
    expect(firstResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const secondResult = await secondCommand.execute(pool)
    expect(secondResult).toMatchObject({ resultType: "MESSAGE_DEDUPLICATED" })

    const messages = await pool.query("SELECT * FROM test.message").then(res => res.rows)
    expect(messages).toHaveLength(1)
    expect(messages[0]).toMatchObject({
        id: firstCommand.id,
        num_attempts: 0,
        content: Buffer.from("hello"),
        channel_name: "alpha",
    })

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 1,
        message_id: firstCommand.id,
        message_dequeue_at: messages[0].dequeue_at,
    })
})

test("MessageCreateCommand doesn't deduplicate messages with the same name if one has been processed", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 0,
        content: Buffer.from("hello"),
        name: "my-message",
    })

    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        delayMs: 0,
        content: Buffer.from("world"),
        name: "my-message",
    })

    const firstResult = await firstCommand.execute(pool)
    expect(firstResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const dequeueResult = await new MessageDequeueCommand({
        schema: SCHEMA,
    }).execute(pool)
    expect(dequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })

    const secondResult = await secondCommand.execute(pool)
    expect(secondResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const messages = await pool.query("SELECT * FROM test.message ORDER BY seq_no").then(res => res.rows)
    expect(messages).toHaveLength(2)

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 2
    })
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
    expect(firstResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const secondResult = await secondCommand.execute(pool)
    expect(secondResult).toMatchObject({ resultType: "MESSAGE_CREATED" })

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

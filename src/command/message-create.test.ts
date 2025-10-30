import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import { MessageCreateCommand, type MessageCreateCommandResultMessageCreated, type MessageCreateCommandResultMessageDropped } from "@src/command/message-create"
import { Queue } from "@src/queue"
import { queueEventDecode } from "@src/queue/event"
import { beforeEach, expect, test } from "bun:test"
import { Pool, type PoolClient } from "pg"

const EVENT_CHANNEL = "events"
const SCHEMA = "test"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: SCHEMA })

const tx = async <T>(fn: (client: PoolClient) => Promise<T>) => {
    const client = await pool.connect()
    try {
        await client.query("BEGIN")
        const result = await fn(client)
        await client.query("COMMIT")
        return result
    } catch (err) {
        await client.query("ROLLBACK")
        throw err
    }
}

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
        channelId: "alpha",
        content: Buffer.from("hello"),
        dequeueAt: 100,
    })

    const client = await pool.connect()
    await client.query(`LISTEN "${EVENT_CHANNEL}"`)

    const events: any[] = []
    const lock = Promise.race([
        new Promise<void>((rs) => setTimeout(() => rs(), 5_000)),
        new Promise<void>((rs) => {
            client.on("notification", (msg) => {
                if (msg.channel === EVENT_CHANNEL) {
                    events.push(queueEventDecode(msg.payload as string))
                    rs()
                }
            })
        })
    ])

    try {
        await tx(async (client) => {
            const now = await client.query("SELECT test.epoch() AS now").then(res => res.rows[0].now as number)
            const result = await command.execute(client) as MessageCreateCommandResultMessageCreated
            const message = await client.query("SELECT * FROM test.message").then(res => res.rows[0])
            const channelState = await client.query("SELECT * FROM test.channel_state").then(res => res.rows[0])

            expect(message).toMatchObject({
                id: result.id.toString(),
                num_attempts: "0",
                content: Buffer.from("hello"),
                channel_id: "alpha",
            })

            expect(channelState).toMatchObject({
                message_id: result.id.toString(),
                current_size: 1,
                message_dequeue_at: "100",
                dequeue_next_at: now.toString(),
            })
        })

        await lock
        client.removeAllListeners("notification")
        expect(events).toHaveLength(1)
        expect(events[0]).toMatchObject({
            eventType: "MESSAGE_CREATED",
        })
    } finally {
        await client.release()
    }
})

test("MessageCreateCommand correctly drops messages when channel is full", async () => {
    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelId: "alpha",
        maxSize: 2
    }).execute(pool)

    const createCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelId: "alpha",
        dequeueAt: 10,
        content: Buffer.from("hello"),
    })

    const firstResult = await createCommand.execute(pool) as MessageCreateCommandResultMessageCreated
    expect(firstResult.resultType).toBe("MESSAGE_CREATED")

    const secondResult = await createCommand.execute(pool) as MessageCreateCommandResultMessageCreated
    expect(secondResult.resultType).toBe("MESSAGE_CREATED")

    const thirdResult = await createCommand.execute(pool) as MessageCreateCommandResultMessageDropped
    expect(thirdResult.resultType).toBe("MESSAGE_DROPPED")
})

test("MessageCreateCommand correctly updates channelState when preempting a \"lower\" priority message", async () => {
    const firstCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelId: "alpha",
        dequeueAt: 10,
        content: Buffer.from("hello"),
    })

    const secondCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelId: "alpha",
        dequeueAt: 5,
        content: Buffer.from("hello"),
    })

    await tx(async (client) => {
        const now = await client.query(
            "SELECT test.epoch() AS now"
        ).then(res => res.rows[0].now as number)

        const firstResult = await firstCommand.execute(client) as MessageCreateCommandResultMessageCreated

        const firstMessage = await client.query(
            "SELECT * FROM test.message WHERE id = $1",
            [firstResult.id]
        ).then(res => res.rows[0])

        const channelStateFirst = await client.query(
            "SELECT * FROM test.channel_state"
        ).then(res => res.rows[0])

        expect(channelStateFirst).toMatchObject({
            id: "alpha",
            current_size: 1,
            dequeue_next_at: now,
            message_dequeue_at: firstMessage.dequeue_at,
            message_id: firstResult.id.toString(),
        })

        const secondResult = await secondCommand.execute(client) as MessageCreateCommandResultMessageCreated

        const secondMessage = await client.query(
            "SELECT * FROM test.message WHERE id = $1",
            [secondResult.id]
        ).then(res => res.rows[0])

        const channelStateSecond = await client.query(
            "SELECT * FROM test.channel_state"
        ).then(res => res.rows[0])

        expect(channelStateSecond).toMatchObject({
            id: "alpha",
            current_size: 2,
            dequeue_next_at: now,
            message_dequeue_at: secondMessage.dequeue_at,
            message_id: secondResult.id.toString(),
        })
    })
})

test("MessageCreateCommand correctly returns the channel size", async () => {
    const command = new MessageCreateCommand({
        schema: SCHEMA,
        channelId: "alpha",
        dequeueAt: null,
        content: Buffer.from("hello"),
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
        channelId: "alpha",
        content: Buffer.from("hello"),
        dequeueAt: dequeueAt,
    })

    const result = await command.execute(pool) as MessageCreateCommandResultMessageCreated
    const message = await pool
        .query("SELECT dequeue_at FROM test.message WHERE id = $1", [result.id])
        .then(res => res.rows[0])

    expect(Number(message.dequeue_at)).toBe(dequeueAt)
})

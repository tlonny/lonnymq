import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import { MessageCreateCommand } from "@src/command/message-create"
import { MessageDeferCommand } from "@src/command/message-defer"
import { MessageDeleteCommand } from "@src/command/message-delete"
import { MessageDequeueCommand, type MessageDequeueCommandResultMessageDequeued } from "@src/command/message-dequeue"
import { Queue } from "@src/queue"
import { sleep } from "bun"
import { beforeEach, expect, test } from "bun:test"
import { Pool } from "pg"

const SCHEMA = "test"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: SCHEMA })

beforeEach(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`)
    await pool.query(`CREATE SCHEMA "${SCHEMA}"`)
    for (const install of queue.install()) {
        await pool.query(install)
    }
})


test("MessageDequeueCommand correctly increments channelState", async () => {
    const messageCreate1Command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 600,
        content: Buffer.from("hello")
    })

    await messageCreate1Command.execute(pool)

    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        releaseIntervalMs: 50,
    }).execute(pool)

    const messageCreate2Command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 600,
        content: Buffer.from("hello")
    })

    await messageCreate2Command.execute(pool)

    const messageDequeueCommand = new MessageDequeueCommand({ schema: SCHEMA })

    const messageDequeueResult = await messageDequeueCommand.execute(pool)
    expect(messageDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })

    const channelState = await pool.query("SELECT * FROM test.channel_state").then(res => res.rows[0])
    expect(channelState).toMatchObject({
        name: "alpha",
        current_size: 2,
        message_id: messageCreate2Command.id,
        active_next_at: new Date(channelState.active_prev_at.getTime() + 50),

    })
})

test("MessageDequeueCommand dequeues messages in the correct order with correct metadata", async () => {
    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        maxConcurrency: 1
    }).execute(pool)

    const messageContents = Array.from({ length: 100 }, (_, i) => `message-${i}`)
    const client = await pool.connect()
    try {
        await client.query("BEGIN")
        for (const content of messageContents) {
            await new MessageCreateCommand({
                schema: SCHEMA,
                lockMs: 10,
                channelName: "alpha",
                content: Buffer.from(content)
            }).execute(client)
        }

        await client.query("COMMIT")
    } catch (e) {
        await client.query("ROLLBACK")
        throw e
    } finally {
        client.release()
    }

    let counter = 0
    let previouslyNotAvailable = false

    while (counter < messageContents.length) {
        const result = await new MessageDequeueCommand({ schema: SCHEMA }).execute(pool)
        if (result.resultType === "MESSAGE_NOT_AVAILABLE") {
            expect(previouslyNotAvailable).toBe(false)
            previouslyNotAvailable = true
            await sleep(20)
            continue
        }

        previouslyNotAvailable = false
        expect(result.message).toMatchObject({ content: Buffer.from(messageContents[counter]) })

        if (result.message.numAttempts === 1) {
            if (counter % 15 === 0) {
                continue
            } else if (counter % 10 === 0) {
                await new MessageDeferCommand({
                    schema: SCHEMA,
                    numAttempts: result.message.numAttempts,
                    id: result.message.id
                }).execute(pool)
            }
        }

        if (counter % 15 === 0) {
            expect(result.message.isUnlocked).toBe(true)
        }

        counter += 1
        await new MessageDeleteCommand({
            schema: SCHEMA,
            id: result.message.id,
            numAttempts: result.message.numAttempts,
        }).execute(pool)
    }
})

test("MessageDequeueCommand correctly increments numAttempts after defer", async () => {
    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        maxConcurrency: 1
    }).execute(pool)

    await new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 10,
        content: Buffer.from("test message")
    }).execute(pool)

    const firstDequeueResult = await new MessageDequeueCommand({ schema: SCHEMA }).execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(firstDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })
    expect(firstDequeueResult.message.numAttempts).toBe(1)

    await new MessageDeferCommand({
        schema: SCHEMA,
        numAttempts: firstDequeueResult.message.numAttempts,
        id: firstDequeueResult.message.id
    }).execute(pool)

    const secondDequeueResult = await new MessageDequeueCommand({ schema: SCHEMA }).execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(secondDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })
    expect(secondDequeueResult.message.numAttempts).toBe(2)
})

test("MessageDequeueCommand correctly sets isUnlocked", async () => {
    await new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        maxConcurrency: 1
    }).execute(pool)

    await new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 0,
        content: Buffer.from("test message")
    }).execute(pool)

    const firstDequeueResult = await new MessageDequeueCommand({ schema: SCHEMA }).execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(firstDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })
    expect(firstDequeueResult.message.isUnlocked).toBe(false)

    const secondDequeueResult = await new MessageDequeueCommand({ schema: SCHEMA }).execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(secondDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })
    expect(secondDequeueResult.message.isUnlocked).toBe(true)
})

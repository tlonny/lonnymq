import { ChannelPolicySetCommand } from "@src/command/channel-policy-set"
import { MessageCreateCommand } from "@src/command/message-create"
import { MessageDequeueCommand } from "@src/command/message-dequeue"
import { Queue } from "@src/queue"
import { beforeEach, expect, test } from "bun:test"
import { Pool } from "pg"

const SCHEMA = "test"
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: SCHEMA })

beforeEach(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`)
    await pool.query(`CREATE SCHEMA "${SCHEMA}"`)
    for (const migration of queue.migrations()) {
        await pool.query(migration)
    }
})

test("MessageDequeueCommand correctly increments channelState", async () => {
    const messageCreate1Command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello")
    })

    const messageCreate1Result = await messageCreate1Command.execute(pool)
    expect(messageCreate1Result).toMatchObject({ resultType: "MESSAGE_CREATED" })

    const channelPolicyCommand = new ChannelPolicySetCommand({
        schema: SCHEMA,
        channelName: "alpha",
        releaseIntervalMs: 50
    })

    await channelPolicyCommand.execute(pool)

    const messageCreate2Command = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        content: Buffer.from("hello")
    })

    const messageCreate2Result = await messageCreate2Command.execute(pool)
    expect(messageCreate2Result).toMatchObject({ resultType: "MESSAGE_CREATED" })

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

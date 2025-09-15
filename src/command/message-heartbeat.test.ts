import { MessageCreateCommand } from "@src/command/message-create"
import { MessageDequeueCommand, type MessageDequeueCommandResultMessageDequeued } from "@src/command/message-dequeue"
import { MessageHeartbeatCommand } from "@src/command/message-heartbeat"
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
    for (const migration of queue.migrations()) {
        await pool.query(migration)
    }
})

test("MessageHeartbeatCommand keeps bumping the unlock_at", async () => {
    await new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 50,
        content: Buffer.from("hello")
    }).execute(pool)

    const messageDequeueCommand = new MessageDequeueCommand({ schema: SCHEMA })

    const messageDequeueResult = await messageDequeueCommand.execute(pool)
    expect(messageDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })

    await sleep(80)

    const messageDequeue2Result = await messageDequeueCommand.execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(messageDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })

    await sleep(80)

    const messageHeartbeatResult = await new MessageHeartbeatCommand({
        schema: SCHEMA,
        id: messageDequeue2Result.message.id,
        numAttempts: messageDequeue2Result.message.numAttempts,
    }).execute(pool)
    expect(messageHeartbeatResult).toMatchObject({ resultType: "MESSAGE_HEARTBEATEDED" })

    const messageDequeue3Result = await messageDequeueCommand.execute(pool)
    expect(messageDequeue3Result).toMatchObject({ resultType: "MESSAGE_NOT_AVAILABLE" })
})

test("MessageHeartbeatCommand fails on invalid numAttempts", async () => {
    await new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 50,
        content: Buffer.from("hello")
    }).execute(pool)

    const messageDequeueCommand = new MessageDequeueCommand({ schema: SCHEMA })
    const messageDequeueResult = await messageDequeueCommand.execute(pool) as MessageDequeueCommandResultMessageDequeued
    expect(messageDequeueResult).toMatchObject({ resultType: "MESSAGE_DEQUEUED" })

    const messageHeartbeatResult = await new MessageHeartbeatCommand({
        schema: SCHEMA,
        id: messageDequeueResult.message.id,
        numAttempts: 0
    }).execute(pool)

    expect(messageHeartbeatResult).toMatchObject({ resultType: "MESSAGE_STATE_INVALID" })
})

test("MessageHeartbeatCommand fails when not locked", async () => {
    const messageCreateCommand = new MessageCreateCommand({
        schema: SCHEMA,
        channelName: "alpha",
        lockMs: 50,
        content: Buffer.from("hello")
    })
    await messageCreateCommand.execute(pool)

    const messageHeartbeatResult = await new MessageHeartbeatCommand({
        schema: SCHEMA,
        id: messageCreateCommand.id,
        numAttempts: 0
    }).execute(pool)

    expect(messageHeartbeatResult).toMatchObject({ resultType: "MESSAGE_STATE_INVALID" })
})

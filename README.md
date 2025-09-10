# LonnyMQ

A high-performance, multi-tenant PostgreSQL message queue implementation for Node.js/TypeScript.

## Features

- High throughput message processing
- Multi-tenant concurrency and capacity constraints
- Durable message processing with automatic recovery
- Flexible message processing retries and deferrals
- Message deduplication
- Queue operations as part of *existing* database transactions
- Database client agnostic
- Granular events via PostgreSQL `NOTIFY`
- Zero dependencies

**Note:** Unlike other queue implementations, LonnyMQ provides direct access to queue methods rather than providing batteries-included Worker/Processor daemons.

## Quick Look

```typescript
import { Queue, type DatabaseClient } from "lonnymq"
import { Pool } from "pg"

const databaseClient = new Pool({ connectionString: process.env.DATABASE_URL })
databaseClient satisfies DatabaseClient

const queue = new Queue({ schema: "lonny" })

// Run migrations first
for (const migration of queue.migrations()) {
    for (const sql of migration.sql) {
        await databaseClient.query(sql, [])
    }
}

// Create messages
for (let ix = 0; ix < 500; ix += 1) {
    await queue
        .channel("myChannel")
        .message
        .create({ 
            content: Buffer.from("Hello"),
            databaseClient,
        })
}

// Process messages
while (true) {
    const dequeueResult = await queue.dequeue({ databaseClient })
    if (dequeueResult.resultType === "MESSAGE_NOT_AVAILABLE") {
        const sleepMs = Math.min(1000, dequeueResult.retryMs ?? 5000)
        await new Promise(resolve => setTimeout(resolve, sleepMs))
        continue
    }

    console.log(dequeueResult.message.content.toString())
    await dequeueResult.message.delete({ databaseClient })
}
```

## Setup & Installation

LonnyMQ can be installed from npm:

```bash
npm install lonnymq
```

Once the package is installed, you need to install the required database schema. LonnyMQ is agnostic to database client and migration process, providing users with an ordered list of migrations - each containing a unique name and SQL fragments to be executed.

```typescript
const queue = new Queue({ schema: "lonny" })
const migrations = queue.migrations()

// Execute migrations (in a transaction for safety)
await databaseClient.query("BEGIN")
try {
    for (const migration of migrations) {
        for (const sql of migration.sql) {
            await databaseClient.query(sql, [])
        }
    }
    await databaseClient.query("COMMIT")
} catch (error) {
    await databaseClient.query("ROLLBACK")
    throw error
}
```

**Note:** Migration SQL is not idempotent and should be executed within a transaction that can be rolled back if an error occurs.

## Channels

Channels provide LonnyMQ's multi-tenancy support. They can be considered lightweight sub-queues that are read from in round-robin fashion. There is no performance penalty for using large numbers of channels, so they can be assigned on a highly granular basis (e.g., per-user) to ensure work is scheduled fairly.

Channels can be configured with concurrency, capacity, and rate limits by setting their "channel policy":

```typescript
await queue
    .channel("my-channel")
    .policy
    .set({ 
        databaseClient,
        maxConcurrency: 1,
        maxSize: 100,
        releaseIntervalMs: 1000
    })

// Remove all constraints:
await queue
    .channel("my-channel")
    .policy
    .clear({ databaseClient })
```

## Message Creation

You can add a message to the queue and assign it to a particular channel using the `create` function:

```typescript
await queue
    .channel("my-channel")
    .message
    .create({
        databaseClient,
        content: Buffer.from("Hello, world"),
        name: "optional-dedup-key",
        lockMs: 30000,  // 30 seconds
        delayMs: 5000   // 5 second delay
    })
```

The `name` argument can be provided for deduplication purposes: if a message that has *never* been dequeued exists with the same name within the same channel, no new message will be created.

## Message Processing

Messages can be fetched for processing by calling `dequeue` on the `Queue` - this locks the message. Once processing is complete, messages must be "finalized" via **deletion** or **deferral** (for further processing in the future).

```typescript

if (dequeueResult.resultType === "MESSAGE_DEQUEUED") {
    const { message } = dequeueResult
    console.log(`Processing message: ${message.id}`)
    console.log(`Content: ${message.content.toString()}`)
    console.log(`Attempts: ${message.numAttempts}`)
    
    try {
        // Process the message...
        await processMessage(message.content)
        
        // Delete on success
        await message.delete({ databaseClient })
    } catch (error) {
        // Defer for retry with updated state
        await message.defer({ 
            databaseClient,
            delayMs: 30000, // Retry in 30 seconds
            state: Buffer.from(JSON.stringify({ error: error.message }))
        })
    }
}
```

When deferring a message, you can optionally specify `delayMs` and `state` arguments. The `delayMs` parameter tells the queue how long to wait before making the message available for reprocessing, and `state` allows you to "save your work" and implement durable and/or repeating/scheduled tasks.

### Graceful Shutdowns and Message Recovery

If your program ends unexpectedly, messages that are currently being processed may become "orphaned" in a locked state - causing channel blockages and reducing throughput. To mitigate this problem, it's essential that you shut down gracefully by catching unhandled exceptions and signals (i.e., `SIGINT`/`SIGTERM`) and finalize all outstanding messages before exiting.

That said, despite our best efforts, if we run out of memory, suffer a power loss, or receive a `SIGKILL`, we will be unable to finalize messages that are currently locked. To mitigate this, we set a `lockMs` during message creation (by default this is 1 hour) which limits the maximum amount of time a message can be locked before becoming available again for dequeue. This facility ensures that regardless of the nature of the shutdown, the queue will always recover automatically.

## Events

Using PostgreSQL `NOTIFY`, we can receive a granular stream of queue events:

  1. `MESSAGE_CREATED`
  2. `MESSAGE_DEFERRED`
  4. `MESSAGE_DELETED`

To enable this feature, ensure the optional `eventChannel` is defined when constructing the SQL migrations.

### Improving on Polling

The simplest approach for processing messages is to call `dequeue` in a loop, backing off with a sleep when no messages are available. The downside of this approach is that we lose reactivity as we increase the polling timeout interval.

To improve reactivity, you can use the `retryMs` returned when failing to dequeue a message. This will either be `null` or tell you how long until the next message becomes available for processing (a message might be deferred for a period of time). Thus, you can tune your sleep to use the minimum of your `retryMs` and a default poll timeout.

Unfortunately, this doesn't help in situations where a message is created or deferred while a worker is sleeping. However, by tracking the `delayMs` provided by the `MESSAGE_CREATED` and `MESSAGE_DEFERRED` events, we can determine the minimum amount of time to sleep until a message becomes available.

```typescript
const queue = new Queue({ schema: "lonny" })
const migrations = queue.migrations({ eventChannel: "EVENTS" })

// LISTEN/NOTIFY only works with a single connection - not on a connection pool.
const client = await databaseClient.connect()
await client.query(`LISTEN "EVENTS"`)
client.on("notification", (msg) => {
    if (msg.channel === "EVENTS") {}
        const event = queueEventDecode(msg.payload as string)
        if(event.eventType === "MESSAGE_CREATED") {
            console.log(`Should wake in ${event.delayMs} ms`)
        } else if(event.eventType === "MESSAGE_DEFERRED") {
            console.log(`Should wake in ${event.delayMs} ms`)
        }
    }
})
```

### Waiting for Job Completion

The `MESSAGE_DELETED` event can be used to create coordination patterns where one part of your application waits for an unrelated job to complete. By listening for deletion events on specific channels or message names, you can implement blocking operations that wait for background work to finish.

```typescript
// Worker process
const client = await databaseClient.connect()
await client.query(`LISTEN "EVENTS"`)

const wait = (messageId: string) : Promise<void> => {
    return new Promise((resolve) => {
        const handler = (msg) => {
            if (msg.channel === "EVENTS") {
                const event = queueEventDecode(msg.payload as string)
                if (event.eventType === "MESSAGE_DELETED" && 
                    event.channelName === "background-jobs" && 
                    event.messageName === messageId) {
                    client.off("notification", handler)
                    resolve()
                }
            }
        }
        client.on("notification", handler)
    })
}

await wait(messageId)
```

## Deadlocks

If all queue actions are isolated to their own transaction, there is zero risk of deadlocks occurring. That being said, it is *possible* to safely bulk-perform the following actions within a single transaction if we ensure they are performed in a consistent lexicographical ordering with respect to channel name and message name (if provided):

- Message create
- Channel policy set  
- Channel policy clear

Beyond the actions specified above, it is manifestly **unsafe** to bulk-perform any of the remaining actions within a single transaction. Each of these actions should be isolated within their **own** transaction:

- Message dequeue
- Message defer
- Message delete

## Batching Operations

When you need to perform multiple safe operations (message creation and channel policy changes) within a single transaction, LonnyMQ provides a batching mechanism that automatically handles proper ordering to prevent deadlocks.

The batch interface mirrors the main queue interface - you call `queue.batch()` to create a batch, then use the same `.channel(name).message.create()` and `.channel(name).policy.set/clear()` methods you're already familiar with. The key difference is that batch operations are queued up and don't execute immediately.

The batch system ensures that all operations are executed in a consistent lexicographical order based on channel name and message name, eliminating the possibility of deadlocks when multiple workers are performing bulk operations simultaneously.

```typescript
const batch = queue.batch()

batch.channel("user-123").message.create({ 
    content: Buffer.from("Welcome email") 
})

batch.channel("user-123").policy.set({
    maxConcurrency: 5,
    maxSize: 1000,
    releaseIntervalMs: 100
})

batch.channel("notifications").message.create({ 
    content: Buffer.from("Daily digest"),
    name: "daily-digest-2025-08-29"
})

batch.channel("analytics").policy.clear()

await batch.execute({ databaseClient })
```

## Database Clients

LonnyMQ is designed to be database client agnostic, requiring only a minimal interface that most PostgreSQL clients already implement. Your database client must provide a single `query` method with this signature:

```typescript
interface DatabaseClient {
    query(sql: string, params: Array<unknown>): Promise<{
        rows: Array<Record<string, unknown>>
    }>
}
```
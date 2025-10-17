# LonnyMQ

A high-performance, multi-tenant PostgreSQL message queue implementation for Node.js/TypeScript. Docs can be found [here](https://tlonny.github.io/lonnymq)

## Features

- High throughput message processing
- Multi-tenant concurrency and rate limits
- Durable message processing.
- Support for retries, recovery and custom back-off strategies.
- Message prioritisation.
- Queue operations as part of *existing* database transactions
- Database client agnostic with optional adapters
- Granular events via PostgreSQL `NOTIFY` - Avoid poling workers!
- Zero dependencies

**Note:** Unlike other queue implementations, LonnyMQ provides direct access to queue methods rather than providing batteries-included Worker/Processor daemons.

## Quick Look

```typescript
import { Queue } from "lonnymq"
import { Pool } from "pg"

const databaseClient = new Pool({ connectionString: process.env.DATABASE_URL })
const queue = new Queue({ schema: "lonny" })

// Install the queue to the database
for (const sql of queue.install()) {
    await databaseClient.query(sql, [])
}

// Create messages
for (let ix = 0; ix < 500; ix += 1) {
    await queue.message.create({ 
        databaseClient,
        content: Buffer.from("Hello")
    })
}

// Process messages
while (true) {
    const dequeueResult = await queue.dequeue({ 
        databaseClient,
        lockMs: 30_000 
    })
    if (dequeueResult.resultType === "MESSAGE_NOT_AVAILABLE") {
        break
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

Once the package is installed, the queue needs to be "installed" to a postgres schema. The requisite SQL for this can be generated via: `queue.install()`.

## Channels

Channels provide LonnyMQ's multi-tenancy support. They can be considered lightweight sub-queues that are read from in round-robin fashion. There is no performance penalty for using large numbers of channels, so they can be assigned on a highly granular basis (e.g., per-user) to ensure work is scheduled fairly.

Channels can be configured with concurrency and rate limits by setting their "channel policy":

```typescript
await queue
    .channel("my-channel")
    .policy
    .set({ 
        databaseClient,
        maxConcurrency: 1,
        releaseIntervalMs: 1000
    })

// Remove all constraints:
await queue
    .channel("my-channel")
    .policy
    .clear({ databaseClient })
```

## Message Creation

You can add a message to the queue using the `create` function. By default, messages are assigned to a unique channel, resulting in basic FIFO behaviour.

```typescript
await queue.message.create({
    databaseClient,
    content: Buffer.from("Hello, world"),
})
```

If you need to assign messages to specific channels (for example, to take advantage of fairness, concurrency or rate limiting features), you can specify the channel explicitly:

```typescript
await queue
    .channel("my-channel")
    .message
    .create({
        databaseClient,
        content: Buffer.from("Hello, world")
    })
```

By default, created messages are immediately available for processing. To set an explicit schedule for when the message should be processed we can pass a `schedule` parameter that either specifies a relative offset or absolute unix timestamp (in ms).

```typescript
await queue.message.create({
        databaseClient,
        content: Buffer.from("Hello, world"),
        schedule: {
            scheduleType: "TIMESTAMP",
            timestamp: Date.now() + 5_000 // 5s in the future
        }
    })

await queue.message.create({
        databaseClient,
        content: Buffer.from("Hello, world"),
        schedule: {
            scheduleType: "OFFSET",
            offsetMs: 5_000 // 5s in the future
        }
    })
```

N.B. `OFFSET` scheduling is relative to the _database_ clock and thus avoid accuracy issues caused by clock drift between worker daemons and the database.

### Message Prioritization

LonnyMQ doesn't use an explicit message priority field for performance reasons. In short, there is no way to find the highest priority message that is also available for dequeue for a particular channel using _just_ an Index Scan.

However, we note that messages that _are_ available for processing are dequeued from their channels in order of their _scheduled_ processing times (oldest first). Thus, we can (ab)use `TIMESTAMP` scheduling by using _historic_ timestamp values as a stand-in for priority (where 0 would be the highest priority message).

N.B. there is no way to _globally_ prioritize a message. Regardless of how a message is prioritized within a channel, the channel will still be accessed in a round robin fashion in accordance with its channel policy (should it exist).

```typescript
const HIGHEST_PRIORITY = 0
const HIGH_PRIORITY = 1
const NORMAL_PRIORITY = 2
const LOW_PRIORITY = 3
const LOWEST_PRIORITY = 4

await queue.message.create({
        databaseClient,
        content: Buffer.from("Hello, world"),
        schedule: {
            scheduleType: "TIMESTAMP",
            timestamp: HIGH_PRIORITY
        }
    })
```

## Message Processing

Messages can be fetched for processing by calling `dequeue` on the `Queue` - this locks the message for a specified duration. Once processing is complete, messages must be "finalized" via **deletion** or **deferral** (for further processing in the future).

```typescript
const dequeueResult = await queue.dequeue({ 
    databaseClient,
    lockMs: 60_000
})

if (dequeueResult.resultType === "MESSAGE_DEQUEUED") {
    const { message } = dequeueResult
    console.log(`Processing message: ${message.id}`)
    console.log(`Content: ${message.content.toString()}`)
    console.log(`State: ${message.state?.toString()}`)
    
    try {
        // Process the message...
        await processMessage(message.content)
        
        // Delete on success
        await message.delete({ databaseClient })
    } catch (error) {
        if (message.numAttempts >= 5) {
            // Too many retries, delete permanently
            await message.delete({ databaseClient })
        } else {
            // Defer for retry with exponential backoff and updated state
            const backoffMs = Math.pow(2, message.numAttempts) * 1000
            await message.defer({ 
                databaseClient,
                schedule: { scheduleType: "OFFSET", offsetMs: backoffMs },
                state: Buffer.from(JSON.stringify({ 
                    error: error.message,
                    lastAttempt: new Date().toISOString()
                }))
            })
        }
    }
} else {
    console.log("No messages available")
}
```

The `lockMs` parameter on `dequeue()` specifies how long a message will remain exclusively locked after being dequeued. While locked, the message is **not available** for subsequent `dequeue()` calls, preventing duplicate processing. If your process crashes or takes longer than expected, the message will automatically become available for dequeue again after the lock expires.

When a message is deferred - it becomes immediately available for re-processing. Similarly to message creation, this behaviour can again be overridden with an optional `schedule` parameter.

**Note:** The above shows just one processing pattern (defer on failure with retry limits). You have complete flexibility in how you handle message processing - you might delete messages immediately, defer them unconditionally, implement different retry strategies based on error types, or use the message metadata (attempts, state, channel) to make sophisticated routing decisions.

### Extending Message Locks with Heartbeats

For messages that take a long time to process, setting a large initial lock is far from ideal. A crash shortly after message dequeue will result in channel throughput being degraded for a significant time (if the channel is concurrency-constrained). To mitigate this, you can set a short initial lock time that can be periodically renewed during message processing via a heartbeat:

```typescript
const dequeueResult = await queue.dequeue({ 
    databaseClient,
    lockMs: 30_000 
})

if (dequeueResult.resultType === "MESSAGE_DEQUEUED") {
    const { message } = dequeueResult
    
    // Start long-running process
    const longTask = processLongRunningTask(message.content)
    
    // Set up heartbeat to extend lock every 20 seconds
    const heartbeatInterval = setInterval(async () => {
        await message.heartbeat({ 
            databaseClient,
            lockMs: 30_000
        })
    }, 20_000)
    
    try {
        await longTask
        await message.delete({ databaseClient })
    } catch (error) {
        await message.defer({ databaseClient, offsetMs: 60_000 })
    } finally {
        clearInterval(heartbeatInterval)
    }
}
```

### Graceful Shutdowns and Message Recovery

If your program ends unexpectedly, messages that are currently being processed may become "orphaned" in a locked state - causing channel blockages and reducing throughput until the lock expires. To mitigate this problem, it's essential that you shut down gracefully by catching unhandled exceptions and signals (i.e., `SIGINT`/`SIGTERM`) and finalize all outstanding messages before exiting.

## Events

Using PostgreSQL `NOTIFY`, we can receive a granular stream of queue events:

  1. `MESSAGE_CREATED`
  2. `MESSAGE_DEFERRED`
  4. `MESSAGE_DELETED`

To enable this feature, ensure the optional `eventChannel` is defined when generating the installation SQL.

```typescript
const install = queue.install({ eventChannel: "EVENTS"})
```

### Improving on Polling

The simplest approach for processing messages is to call `dequeue` in a loop, backing off with a sleep when no messages are available. The downside of this approach is that we lose reactivity as we increase the polling timeout interval.

```typescript
// Basic polling approach
while (true) {
    const result = await queue.dequeue({ databaseClient, lockMs: 30_000 })
    
    if (result.resultType === "MESSAGE_NOT_AVAILABLE") {
        await sleep(5_000) 
        continue
    }
    
    // Process message...
    await processMessage(result.message)
    await result.message.delete({ databaseClient })
}
```

To improve reactivity, you can use the events system to track when new messages become available. By listening for `MESSAGE_CREATED` and `MESSAGE_DEFERRED` events and tracking their `offsetMs`, you can determine the optimal time to retry dequeuing:

```typescript
// LISTEN/NOTIFY only works with a single connection - not on a connection pool.
const client = await databaseClient.connect()
await client.query(`LISTEN "EVENTS"`)

let nextWakeTime = Date.now()

client.on("notification", (msg) => {
    if (msg.channel === "EVENTS") {
        const event = queueEventDecode(msg.payload as string)
        if (event.eventType === "MESSAGE_CREATED" || event.eventType === "MESSAGE_DEFERRED") {
            const messageAvailableAt = Date.now() + event.offsetMs
            nextWakeTime = Math.min(nextWakeTime, messageAvailableAt)
        }
    }
})
```

### Waiting for Job Completion

The `MESSAGE_DELETED` event can be used to create coordination patterns where one part of your application waits for an unrelated job to complete. By listening for deletion events on specific message IDs, you can implement blocking operations that wait for background work to finish.

## Deadlocks

If all queue actions are isolated to their own transaction, there is zero risk of deadlocks occurring. That being said, it is *possible* to safely bulk-perform the following actions within a single transaction if we ensure they are performed in a consistent ordering with respect to the target channel name:

- Message create
- Channel policy set  
- Channel policy clear

Beyond the actions specified above, it is manifestly **unsafe** to bulk-perform any of the remaining actions within a single transaction. Each of these actions should be isolated within their **own** transaction:

- Message dequeue
- Message defer
- Message delete
- Message heartbeat

## Database Clients

LonnyMQ is designed to be database client agnostic, requiring only a minimal interface that most PostgreSQL clients already implement. Your database client must provide a single `query` method with this signature:

```typescript
interface DatabaseClient {
    query(sql: string, params: Array<unknown>): Promise<{
        rows: Array<Record<string, unknown>>
    }>
}
```

### Database Client Adapters

For database clients that don't match the expected interface exactly, LonnyMQ provides an adapter system to improve the developer experience. You can provide an adapter function when creating a Queue:

```typescript
import { Queue } from "lonnymq"

const queue = new Queue<NonCompliantDatabaseClient>({ 
    schema: "lonny",
    adaptor: (client : NonCompliantDatabaseClient) => ({
        query: async (sql, params) => {
            // Adapt the client's interface to match DatabaseClient
            const result = await client.executeQuery(sql, params)
            return { rows: result.data }
        }
    })
})
```
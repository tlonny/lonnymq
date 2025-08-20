# LonnyMQ

A high performance, multi-tenant Postgres message queue implementation for NodeJS/Typescript.

## Features

  - High throughput.
  - Multi-tenant concurrency and capacity constraints.
  - Durable message processing.
  - Flexible message processing retries/deferrals.
  - Message de-duplication.
  - Queue actions as part of *existing* database transactions.
  - Database client agnostic.
  - Instant reactivity via `LISTEN/NOTIFY`.
  - Zero dependencies.

N.B. unlike other queue implementations, LonnyMQ provides direct access to queue methods vs. providing batteries-included Worker/Processor daemons. 

## Quick Look

```typescript
import { Queue, type DatabaseClient } from "lonnymq"
import { Pool } from "pg"

const databaseClient = new Pool({ connectionString: process.env.DATABASE_URL })
databaseClient satisfies DatabaseClient

const queue = new Queue("lonny")

// Helper function for sleeping
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

for(let ix = 0; ix < 500; ix += 1) {
    await queue
        .channel("myChannel")
        .message
        .create({ 
            content: "Hello", 
            databaseClient,
        })
}

while(true) {
    const dequeueResult = await queue.dequeue({ databaseClient })
    if(dequeueResult.resultType === "MESSAGE_NOT_AVAILABLE") {
        await sleep(Math.min(1_000, dequeueResult.retryMs || 1_000))
        continue
    }

    console.log(dequeueResult.message.content)
    await dequeueResult.message.delete({ databaseClient })
}
```

## Setup & Installation

LonnyMQ can be installed from npm via:

```bash
npm install lonnymq
```

Once the package is installed, we need to install the requisite DB machinery. LonnyMQ is agnostic to DB client/migration process and thus simply provides users an ordered list of "Migrations" - each containing a unique name and some SQL fragments to be executed.

```typescript
const queue = new Queue("lonny")
const migrations = queue.migrations({ useWake: false })
```

N.B. Migration SQL is not idempotent and thus these migrations should be executed in the context of a transaction that can be rolled back.

## Channels

Channels are the mechanism by which LonnyMQ provides multi-tenancy support. They can be considered lightweight sub-queues that are read from in a round-robin fashion. There is no performance penalty associated with using large numbers of channels and thus can be assigned on a highly granular (i.e. per-user) basis to ensure work is scheduled fairly.

Channels can be configured with concurrency and capacity limits by setting their "channel policy". Available policy options include:

- `maxConcurrency`: Maximum number of messages that can be processed concurrently from this channel
- `maxSize`: Maximum number of messages that can be queued in this channel
- `releaseIntervalMs`: Minimum interval (in milliseconds) between message releases from this channel

```typescript
await queue
    .channel("my-channel")
    .policy
    .set({ 
        maxConcurrency: 1, 
        maxSize: null, 
        releaseIntervalMs: 1000,
        databaseClient 
    })

// Remove all constraints:
await queue
    .channel("my-channel")
    .policy
    .clear({ databaseClient })
```

## Message creation

We can add a message to the queue (and assign it to a particular channel) with the `create` function:

```typescript
await queue
    .channel("my-channel")
    .message
    .create({
        databaseClient,
        content: "Hello, world"
    })
```

A `name` argument can be provided for de-duplication purposes: if a message that has _never_ been dequeued exists with the same name, and within the same channel, no new message will be created.

## Message processing

A message can be fetched for processing by calling `dequeue` on the `Queue` - locking the message. Once processing has completed, messages can then be "finalized" via **deletion** or **deferral** (for further processing in the future).

When deferring a message, we can optionally specify `delayMs` and `state` arguments. `delayMs` tells the queue how long to wait before allowing the message to be re-processed, and `state` allows us to "save our working" and implement durable and/or repeating/scheduled tasks.

### Graceful shutdowns and message sweeping

If your program ends unexpectedly, messages that are in the middle of being processed may well be "orphaned" in a locked state - causing channel blockages and reducing throughput. To mitigate this problem, it is imperative that we gracefully shutdown by catching unhandled exceptions and signals (i.e. `SIGINT`/`SIGTERM`) - finalizing all outstanding messages prior to exiting.

That said, despite our best efforts, should we run out of memory, suffer a loss of power, or receieve a `SIGKILL`, we will be unable to finalize messages that are currently locked. To mitigate this, we set a `lockMs` during message creation (by default this is 1 hour) which limits the maximum amount of time a message can be locked before being available again for dequeue. This facilities ensures that no matter the nature of the shutdown, the queue will always un-clog itself.

## Improvements on polling

The simplest approach for processing messages is to call `dequeue` in a loop, and backing off with a sleep when no messages are available. The downside with this approach is that we lose reactivity as we increase the polling timeout interval.

To improve reactivity, we can use the `retryMs` returned when we fail to dequeue a message. This will either be `null` or tell us how long until the next message is available for processing (a message might be deferred for a period time). Thus, we can tune our sleep to use the minimum of our `retryMs` and a default poll timeout.

Unfortunately, this doesn't help us in situations where a message is created/deferred while a worker is sleeping. However, if we deploy LonnyMQ, with the `useWake` parameter enabled, message creations and deferrals will trigger a payload to the `queue.wakeChannel()` Postgres channel with the amount of milliseconds until said message becomes available for processing encoded as a string payload.

```typescript
const queue = new Queue("lonny")
const migrations = queue.migrations({ useWake: true })

// LISTEN/NOTIFY only works with a single connection - not on a connection pool.
const client = await pool.connect()
await client.query(`LISTEN "${queue.wakeChannel()}"`)
client.on("notification", (msg) => {
    if (msg.channel === queue.wakeChannel()) {
        const delayMs = parseInt(msg.payload as string, 10)
        console.log(`Should wake in ${delayMs} ms`)
    }
})
```

## Deadlocks

If all queue actions are isolated to their own transaction, there is 0 risk of deadlocks occurring. That being said, it is _possible_ to safely bulk perform the following actions within a single transaction if we ensure they are performed in a consistent lexicographical ordering with respect to channel name and message name (if provided):

  - Message create
  - Channel policy set
  - Channel policy clear

Beyond the actions specified above, it is manifestly **unsafe** to bulk-perform any of the remaining actions within a single transaction. Each of these actions should be isolated within their **own** transaction:

 - Message dequeue
 - Message defer
 - Message delete

To help with ensuring commands are ordered consistently, we can create a "Batch" object by calling:

```typescript
const batch = queue.batch()
```

This batch object provides a familiar API for message creation and channel policy mutations, but doesn't execute the underlying commands until the underlying batch is explicitly "executed". 

```typescript
const results = [
    batch.channel("foo").message.create({ content: "hi" }),
    batch.channel("bar").policy.clear(),
    batch.channel("bar").message.create({ content: "hi", name: "foo" }),
    batch.channel("bar").message.create({ content: "hi" }),
]
```
Prior to execution, the batch object will perform a sort to ensure actions are ordered consistently

```typescript
await batch.execute({ databaseClient })

// Get the 3rd command that was submitted to the batch
console.log(await results[2].get())
```
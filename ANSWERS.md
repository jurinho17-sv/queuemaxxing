# Additional questions

The four questions from the assessment. The code they refer to is explained in
[WALKTHROUGH.md](WALKTHROUGH.md), and the reasoning behind what was and was not built is in
[DECISIONS.md](DECISIONS.md).

---

## How do you handle replay messages?

"Replay" means two different things, and the queue answers them differently today.

**Redelivery of a message a consumer failed to process.** It does not happen. `Dequeue`
fsyncs the `del` record before returning the message, which makes delivery *at-most-once*.
Nothing is ever delivered twice, and a consumer that dies mid-processing loses that message.
That is a deliberate trade rather than an oversight. The assessment specifies durability of
stored data and concurrency, and says nothing about acknowledgements, so I kept the semantics
narrow and said so.

Widening it to at-least-once is a small change on the existing structure, which is the point.
Dequeue writes a `lease` record carrying a visibility deadline instead of a `del`, the message
moves to a third heap keyed on that deadline, and the `del` is written only when the consumer
acks. Expired leases promote back to ready through the same `promote()` loop the delayed heap
already uses. Consumers then have to be idempotent, and the stable message `id` already in
every message is the dedup key they would use.

**Re-reading a history of messages.** The log already *is* that history. What the queue lacks
is a way to read it without consuming, plus a retention policy that stops compaction from
discarding delivered records. Sequence numbers are already monotonic and durable, which is
the part that is hard to retrofit. Adding a read cursor over the log is the same change that
turns this into pub/sub.

---

## How would you refactor your queue into a Pub/Sub?

The difference is not the transport. It is who owns the read position. A queue destroys a
message on read, while a topic has to let N independent subscribers each see everything.

1. **Stop writing tombstones.** `del` records exist because a queue forgets. A topic
   remembers, so the log becomes the retained record and `Dequeue` no longer mutates it.
2. **Give each subscriber a durable cursor.** `data/<topic>/cursors/<sub>.json` holds the
   last acked sequence number, written with the same temp-fsync-rename dance `meta.json`
   already uses. Reading for a subscriber means taking the next record after its cursor.
   Fan-out costs nothing extra because the log is shared and only cursors differ.
3. **Swap compaction for retention.** Instead of dropping delivered records, drop records
   older than a retention window or below `min(cursors)`.
4. **Narrow the ordering policy.** This is the part to be upfront about. A log is inherently
   FIFO by offset. Priority and LIFO do not survive fan-out cleanly, since each subscriber
   would need its own heap over the shared log rather than a shared cursor. So a pub/sub mode
   would offer FIFO and delay, and priority and LIFO would stay queue-mode features.

That design is the Kafka consumer-group model, arrived at from the same primitives.

---

## If you had more time, what other features would you add?

In the order I would actually build them.

1. **Ack, visibility timeout, and a dead-letter queue.** At-most-once is the largest gap
   between this and something I would put real work on. Sketched above, and a DLQ after N
   delivery attempts falls out of the same lease record.
2. **Group commit.** Throughput is currently one fsync per message. Batching concurrent
   writers into a single fsync raises it by roughly an order of magnitude without weakening
   the guarantee, since every writer still waits for a real sync before its response.
3. **Long polling on `pop`.** Consumers busy-poll today. Holding the request open until a
   message arrives or a timeout elapses removes that, and it is a prerequisite for anything
   latency-sensitive.
4. **Segmented logs.** Rolling the log at a size threshold means compaction rewrites one
   segment instead of the whole file, and recovery can read segments in parallel.
5. **Batch enqueue and dequeue.** One request, many messages, one fsync.
6. **Metrics.** Queue depth, age of the oldest ready message, and fsync latency are the three
   numbers that tell you whether a queue is healthy.

---

## Why would users choose your queue over incumbents like Amazon SQS, RabbitMQ or Apache Pulsar?

For most production workloads they should not, and it is worth saying so plainly. This is a
single node with no replication, so it survives process death but not disk death.

The case for it starts with operational weight. One static binary and one directory is the
whole deployment. SQS requires AWS and bills per request, RabbitMQ brings an Erlang runtime
and a cluster to operate, and Pulsar brings BookKeeper and ZooKeeper. This runs anywhere Go
runs, including embedded directly in another process as a library, with no network hop at all.

The ordering combination is the other reason. SQS offers FIFO queues and no priority, and
priority is emulated by running several queues and polling them in order. RabbitMQ has
priority queues and a delayed message plugin, but no LIFO. Pulsar is a log, so it is
offset-ordered by construction. Having priority, LIFO, and delay compose behind one policy in
one queue is not something the incumbents expose, and it is what the assessment asked for.

There is a smaller point that matters more than it sounds. The whole ordering model is one
function and the whole durability model is one file format, so when a queue misbehaves in
production you can read the rule instead of inferring it from documentation.

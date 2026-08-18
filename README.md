# queuemaxxing

A durable HTTP queue in Go whose ordering is configurable: FIFO or LIFO, with optional
priority, with optional delay. A queue can be a plain FIFO, a priority FIFO, a priority
LIFO, or any of those with a delay.

No database, no embedded store, no external broker. Each queue is one append-only file
that the process fsyncs on every write and replays on startup. The only dependency is the
Go standard library.

Submitted by **Ju Ho Kim** for the Data Engineer Intern position at Artie, in response to the
[technical assessment](docs/assessment.md). Built with Claude Code;
[DECISIONS.md](DECISIONS.md) records the design calls and the alternatives rejected, which
parts of the reasoning were mine, and what was verified how.

---

## Quickstart

```bash
go build -o bin/queued ./cmd/queued   # the queue server
go build -o bin/qctl   ./cmd/qctl     # a client application for it

./bin/queued -addr :8080 -data ./data &

./bin/qctl create orders -order fifo -priority
./bin/qctl send orders "checkout-1" -priority 1
./bin/qctl send orders "refund"     -priority 9
./bin/qctl drain orders
# seq=1    priority=9   refund
# seq=0    priority=1   checkout-1
```

`./scripts/demo.sh` runs the whole story end to end against a real server: every ordering
mode, a delay, a `kill -9` and restart, and four concurrent consumers.

---

## How ordering works

![Ordering model](docs/img/ordering-model.png)
<sub>source: [docs/diagrams/ordering-model.drawio](docs/diagrams/ordering-model.drawio)</sub>

The three features in the assessment collapse into two mechanisms.

**Priority and FIFO/LIFO are a sort.** One comparator expresses every combination:

```go
func (p Policy) before(a, b *Message) bool {
	if p.Priority && a.Priority != b.Priority {
		return a.Priority > b.Priority   // higher priority first
	}
	if p.Order == FIFO {
		return a.Seq < b.Seq             // oldest first
	}
	return a.Seq > b.Seq                 // newest first
}
```

**Delay is not part of the sort.** It decides whether a message is a candidate at all. A
message carries a `visible_at` timestamp; until that time it waits in a second heap ordered
by `visible_at`, and `promote()` moves it across when it comes due. This follows
[Amazon SQS delay queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html),
where the clock starts when a message is *added* rather than when it is consumed.

Because `visible_at` is a stored timestamp rather than a running timer, a pending delay
survives a restart with nothing to reconstruct.

| Policy | Behaviour | The assessment's words |
|---|---|---|
| `{order: fifo}` | oldest first | FIFO |
| `{order: lifo}` | newest first | LIFO |
| `{order: fifo, priority: true}` | priority, then oldest | "a priority FIFO" |
| `{order: lifo, priority: true, delay_seconds: 30}` | priority, then newest, hidden 30s | "a delay, priority LIFO queue" |

---

## How durability works

![Write path](docs/img/write-path.png)
<sub>source: [docs/diagrams/write-path.drawio](docs/diagrams/write-path.drawio)</sub>

Storage cannot be delegated, so `internal/wal` is the storage engine. Each queue owns one
file:

```
data/orders/meta.json    the policy, written atomically (temp file, fsync, rename)
data/orders/wal.log      [4B length][4B CRC32][JSON record] ...
```

Two record types are written: `put` when a message is enqueued, `del` when it is delivered.
Replaying them in order reconstructs the live set exactly.

**Writes are ahead of memory.** `Enqueue` appends and fsyncs the `put` record before
touching the in-memory heaps, and `Dequeue` appends and fsyncs the `del` record before
removing the message. A crash between those two steps is safe in both directions: the log
is the truth, and memory is a cache of it.

**A `201` means the bytes are on disk.** The HTTP response is written after `fsync(2)`
returns, so a client that got a `201` can rely on the message surviving a power loss.

**A torn tail is discarded on replay.** A crash can leave a half-written record at the end
of the file. Replay stops at the first record whose length or CRC32 does not check out,
truncates from there, and continues from a clean boundary. That record can only ever be the
last one, because an earlier record already returned from fsync.

**The log is compacted.** Every message is eventually deleted, so a log that is only
appended to grows forever, and since startup replays the whole log, recovery time would
grow with it. Once the log is at least 1024 records and at least half of them are
superseded, it is rewritten from the live index into a temporary file, fsynced, and moved
into place with `rename(2)`, which is atomic.

---

## How concurrency works

Each queue has one `sync.Mutex` covering both its log and its heaps. Every operation writes
to both, so there are no read-only callers worth separating out, and holding a single lock
across both is what makes "append, then update memory" atomic from the outside. No consumer
can observe a message that is not yet durable, and no message can go to two consumers.

The broker's own lock only guards the name-to-queue map, so traffic on different queues
runs in parallel without contending.

The honest cost: one fsync per message bounds a single queue's throughput to the disk's
sync rate. Group commit is the fix, and it is the first item in the "more time" list below.

---

## HTTP API

| | | |
|---|---|---|
| `POST` | `/queues` | create a queue |
| `GET` | `/queues` | list queues with their depths |
| `GET` | `/queues/{name}` | one queue's policy and depth |
| `DELETE` | `/queues/{name}` | delete a queue and its log |
| `POST` | `/queues/{name}/messages` | enqueue |
| `POST` | `/queues/{name}/messages/pop` | dequeue |

```bash
curl -X POST localhost:8080/queues \
  -d '{"name":"orders","order":"lifo","priority":true,"delay_seconds":30}'

# delay_seconds here overrides the queue default for this message only,
# in the spirit of an SQS message timer.
curl -X POST localhost:8080/queues/orders/messages \
  -d '{"body":"refund","priority":9,"delay_seconds":0}'

curl -X POST localhost:8080/queues/orders/messages/pop
```

`pop` answers `204 No Content` when nothing is deliverable. That is deliberately distinct
from `404`: the queue may hold plenty of messages that are still inside their delay.

---

## Tests

```bash
go test ./... -race
```

| Test | What it proves |
|---|---|
| `TestPolicyOrdering` | all four policy combinations deliver in the right order |
| `TestDelayHidesMessageUntilVisible` | a delayed message is stored but not deliverable |
| `TestPerMessageDelayOverridesQueueDefault` | message-level delay beats the queue default |
| `TestStateSurvivesRestart` | reopening recovers exactly the undelivered messages |
| `TestDelayedMessageSurvivesRestart` | a pending delay comes back still pending |
| `TestConcurrentConsumersNeverDuplicate` | 8 consumers, 500 messages, no message twice |
| `TestConcurrentProducersAndConsumers` | nothing is lost or duplicated under mixed load |
| `TestCompactionShrinksLog` | the log is rewritten and still describes the right state |
| `TestReplayDiscardsTornTail` | a half-written record is truncated, the log stays usable |
| `TestReplayStopsAtBadChecksum` | corruption is not handed back as valid data |

---

## Additional questions

### How do you handle replay messages?

"Replay" means two different things, and the queue answers them differently today.

**Redelivery of a message a consumer failed to process.** It does not happen. `Dequeue`
fsyncs the `del` record before returning the message, which makes delivery *at-most-once*:
nothing is ever delivered twice, and a consumer that dies mid-processing loses that message.
That is a deliberate trade rather than an oversight. The assessment specifies durability of
stored data and concurrency, and says nothing about acknowledgements, so I kept the semantics
narrow and said so.

Widening it to at-least-once is a small change on the existing structure, which is the
point: dequeue writes a `lease` record carrying a visibility deadline instead of a `del`,
the message moves to a third heap keyed on that deadline, and the `del` is written only when
the consumer acks. Expired leases promote back to ready through the same `promote()` loop
the delayed heap already uses. Consumers then have to be idempotent, and the stable message `id`
already in every message is the dedup key they would use.

**Re-reading a history of messages.** The log already *is* that history; what the queue
lacks is a way to read it without consuming, plus a retention policy that stops compaction
from discarding delivered records. Sequence numbers are already monotonic and durable, which
is the part that is hard to retrofit. Adding a read cursor over the log is the same change
that turns this into pub/sub.

### How would you refactor your queue into a Pub/Sub?

The difference is not the transport, it is who owns the read position. A queue destroys a
message on read; a topic has to let N independent subscribers each see everything.

1. **Stop writing tombstones.** `del` records exist because a queue forgets. A topic
   remembers, so the log becomes the retained record and `Dequeue` no longer mutates it.
2. **Give each subscriber a durable cursor.** `data/<topic>/cursors/<sub>.json` holds the
   last acked sequence number, written with the same temp-fsync-rename dance `meta.json`
   already uses. Reading for a subscriber means taking the next record after its cursor.
   Fan-out costs nothing extra because the log is shared and only cursors differ.
3. **Swap compaction for retention.** Instead of dropping delivered records, drop records
   older than a retention window or below `min(cursors)`.
4. **Narrow the ordering policy.** This is the part to be upfront about: a log is
   inherently FIFO by offset. Priority and LIFO do not survive fan-out cleanly, since each
   subscriber would need its own heap over the shared log rather than a shared cursor. So a
   pub/sub mode would offer FIFO and delay, and priority and LIFO would stay queue-mode
   features.

That design is the Kafka consumer-group model, arrived at from the same primitives.

### If you had more time, what other features would you add?

In the order I would actually build them.

1. **Ack, visibility timeout, and a dead-letter queue.** At-most-once is the largest gap
   between this and something I would put real work on. Sketched above; a DLQ after N
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
6. **Metrics.** Queue depth, age of the oldest ready message, and fsync latency are the
   three numbers that tell you whether a queue is healthy.

### Why would users choose your queue over incumbents like Amazon SQS, RabbitMQ or Apache Pulsar?

For most production workloads they should not, and it is worth saying so plainly. This is a
single node with no replication, so it survives process death but not disk death.

The case for it starts with operational weight. One static binary and one directory is the
whole deployment. SQS requires AWS and bills per request, RabbitMQ brings an Erlang runtime
and a cluster to operate, and Pulsar brings BookKeeper and ZooKeeper. This runs anywhere Go
runs, including embedded directly in another process as a library, with no network hop at all.

The ordering combination is the other reason. SQS offers FIFO queues and no priority;
priority is emulated by running several queues and polling them in order. RabbitMQ has
priority queues and a delayed message plugin, but no LIFO. Pulsar is a log, so it is
offset-ordered by construction. Having priority, LIFO, and delay compose behind one policy in
one queue is not something the incumbents expose, and it is what the assessment asked for.

There is a smaller point that matters more than it sounds. The whole ordering model is one
function and the whole durability model is one file format, so when a queue misbehaves in
production you can read the rule instead of inferring it from documentation.

---

## Scope

Deliberately not built: acknowledgements and visibility timeouts, dead-letter queues,
authentication, clustering or replication, long polling, batch operations, and metrics. Each
one is a reasonable thing for a queue to have and none of them is in the assessment. Where
they matter, they are answered above rather than half-built.

## Layout

```
cmd/queued/          the HTTP server
cmd/qctl/            a client application: create, send, pop, drain, worker
internal/wal/        the storage engine: framing, fsync, replay, compaction
internal/queue/      policy (the comparator), the two-heap index, the broker
internal/httpapi/    routes and JSON
scripts/demo.sh      end-to-end demonstration of every property
docs/assessment.md   the assessment, transcribed
docs/diagrams/       diagram sources (.drawio)
WALKTHROUGH.md       a guided read of the code, in the order it makes sense to read it
DECISIONS.md         decisions, rejected alternatives, and the verification record
```

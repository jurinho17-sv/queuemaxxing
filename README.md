# queuemaxxing

A durable HTTP queue in Go with configurable ordering. Every queue is FIFO or LIFO, with
optional priority and optional delay, so it can be a plain FIFO, a priority FIFO, a priority
LIFO, or any of those with a delay. No database, no embedded store, no external broker. Each
queue is one append-only file that the process fsyncs on every write and replays on startup,
and the Go standard library is the only dependency.

My name is **Ju Ho Kim** and I'm applying to Artie for a Data Engineering internship role. I
utilized Claude Code. [DECISIONS.md](DECISIONS.md) records the design calls and the
alternatives rejected, which parts of the reasoning were mine, and what was verified how.
**[ANSWERS.md](ANSWERS.md) answers the four additional questions**, and
[WALKTHROUGH.md](WALKTHROUGH.md) is a guided read of the code, file by file.

## Quickstart

```bash
go build -o bin/ ./cmd/...            # queued, the server, and qctl, a client for it
./bin/queued -addr :8080 -data ./data &

./bin/qctl create orders -order fifo -priority
./bin/qctl send orders "checkout-1" -priority 1
./bin/qctl send orders "refund"     -priority 9
./bin/qctl drain orders
# seq=1    priority=9   refund
# seq=0    priority=1   checkout-1
```

`./scripts/demo.sh` runs the whole story end to end against a real server. Every ordering
mode, a delay, a `kill -9` and restart, and four concurrent consumers.

## How ordering works

![Ordering model](docs/img/ordering-model.png)

The three features in the assessment collapse into two mechanisms.

**Priority and FIFO/LIFO are a sort.** One comparator expresses every combination.

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
message carries a `visible_at` timestamp and waits in a second heap ordered by it until
`promote()` moves it across. This follows
[Amazon SQS delay queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html),
where the clock starts when a message is *added* rather than when it is consumed. A stored
timestamp beats a running timer, because a pending delay then survives a restart intact.

| Policy | Behaviour | The assessment's words |
|---|---|---|
| `{order: fifo}` | oldest first | FIFO |
| `{order: lifo}` | newest first | LIFO |
| `{order: fifo, priority: true}` | priority, then oldest | "a priority FIFO" |
| `{order: lifo, priority: true, delay_seconds: 30}` | priority, then newest, hidden 30s | "a delay, priority LIFO queue" |

## How durability works

![Write path](docs/img/write-path.png)

Storage cannot be delegated, so `internal/wal` is the storage engine. Each queue owns one
directory.

```
data/orders/meta.json    the policy, written atomically (temp file, fsync, rename)
data/orders/wal.log      [4B length][4B CRC32][JSON record] ...
```

Two record types are written. A `put` when a message is enqueued, a `del` when it is
delivered. Replaying them in order reconstructs the live set exactly.

**Writes are ahead of memory.** `Enqueue` fsyncs the `put` record before touching the heaps,
and `Dequeue` fsyncs the `del` record before removing the message. A crash between those two
steps is safe in both directions, because the log is the truth and memory is a cache of it. A
`201` goes out after `fsync(2)` returns, so a client that got one can rely on the message.

**A torn tail is discarded on replay.** A crash can leave a half-written record at the end of
the file. Replay stops at the first record whose length or CRC32 does not check out and
truncates from there. That record can only ever be the last one, because every earlier record
already returned from fsync.

**The log is compacted.** Every message is eventually deleted, so an append-only log grows
forever, and startup replays the whole log, so recovery time would grow with it. Once the log
is at least 1024 records and at least half are superseded, it is rewritten from the live index
into a temporary file, fsynced, and moved into place with an atomic `rename(2)`.

## How concurrency works

Each queue has one `sync.Mutex` covering both its log and its heaps. Every operation writes to
both, so holding a single lock across the pair is what makes "append, then update memory"
atomic from the outside. No consumer can observe a message that is not yet durable, and no
message can go to two consumers. The broker's own lock guards only the name to queue map, so
traffic on different queues runs in parallel without contending. The honest cost is that one
fsync per message bounds a single queue's throughput to the disk's sync rate.

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
curl -X POST localhost:8080/queues -d '{"name":"orders","order":"lifo","priority":true,"delay_seconds":30}'
curl -X POST localhost:8080/queues/orders/messages -d '{"body":"refund","priority":9,"delay_seconds":0}'
```

A message may carry its own `delay_seconds`, overriding the queue default, like an SQS message
timer. `pop` answers `204 No Content` when nothing is deliverable, which is distinct from
`404`, because the queue may hold messages still inside their delay.

## Tests

`go test ./... -race` runs ten tests, covering the four policy combinations, delay
visibility, recovery after restart, concurrent consumers under the race detector, compaction,
and a torn or corrupt log. [WALKTHROUGH.md](WALKTHROUGH.md#8-what-the-tests-are-for) maps
every claim above to its test, plus the six breakages I used to confirm they fail properly.

## Scope

Deliberately not built. Acknowledgements and visibility timeouts, dead-letter queues,
authentication, clustering or replication, long polling, batch operations, and metrics. Every
one is reasonable for a queue to have and none is in the assessment, so where they matter they
are answered in [ANSWERS.md](ANSWERS.md) rather than half-built.

```
cmd/queued/          the HTTP server
cmd/qctl/            a client application, with create, send, pop, drain and worker
internal/wal/        the storage engine, framing, fsync, replay, compaction
internal/queue/      policy (the comparator), the two-heap index, the broker
internal/httpapi/    routes and JSON
scripts/demo.sh      end-to-end demonstration of every property
docs/diagrams/       diagram sources (.drawio), exported to docs/img
ANSWERS.md           the four additional questions
WALKTHROUGH.md       a guided read of the code
DECISIONS.md         decisions, rejected alternatives, and the verification record
```

# Decisions

The record of what was decided, what was rejected, and how the result was checked.
[WALKTHROUGH.md](WALKTHROUGH.md) explains how the code works. This file explains why it is
this code and not some other code.

---

## How this was built

I built this with Claude Code, in a directed loop. I set the scope rule and the boundaries,
the model wrote most of the Go, and I reviewed and ran everything.

I am saying so plainly because the interesting question is not whether a tool was used. It
is whether the person submitting the work can defend it, meaning the design decisions, the
trade-offs, the failure modes, and the parts that are deliberately missing. That is what
this file and [WALKTHROUGH.md](WALKTHROUGH.md) are for, and it is why the walkthrough
argues each decision from the assessment text rather than describing the code back to you.

The parts that were mine rather than the tool's are marked below.

---

## The rule that governed scope

Every candidate feature went through three questions before any code was written.

- **A.** Named in the assessment → build it.
- **B.** Not named, but a named requirement fails without it → build it.
- **C.** Only "a real queue would obviously have this" → do not build it, answer it in
  writing.

C is the one that does the work. The assessment has an *Additional questions* section that
asks what I would add given more time, which means the document itself designates where
ambition belongs. Every feature added to the code is a feature subtracted from that answer.

Worked examples are in [WALKTHROUGH.md §1](WALKTHROUGH.md#1-deciding-what-to-build).

---

## Decisions

| # | Decision | Alternatives | Why |
|---|---|---|---|
| 1 | **Go** | Python, TypeScript | Matches the stack Artie actually runs (the `artie-labs` repos are Go, Kafka, Debezium). The standard library covers HTTP, concurrency and `fsync`, so the project has zero third-party dependencies. `go test -race` turns the concurrency requirement into something demonstrable rather than asserted. |
| 2 | **Hand-written append-only log** | SQLite, BoltDB, an embedded KV store | Forced by "storage cannot be delegated", and also correct on the merits, because a queue's access pattern is append-and-scan, which is what a log is for. A B-tree would buy random access the queue never performs. |
| 3 | **At-most-once, no ack or visibility timeout** | in-flight leases, redelivery, at-least-once | Rule C. The requirements name durability of stored data and concurrency, and say nothing about acknowledgement. The SQS page the assessment links to spends a paragraph distinguishing a *delay* (hidden when added) from a *visibility timeout* (hidden when consumed), which tells you which of the two is in scope. Answered in the README with the exact record type that would change it. |
| 4 | **Log compaction included** | leave it out, mention it as future work | Rule B. "Protected from application restarts" is an explicit requirement, and startup replays the whole log. A queue deletes every message it delivers, so without compaction the log grows forever and recovery time grows with it, so the named requirement stops holding over time. |
| 5 | **Priority as an explicit flag** | always on, with default 0 | Behaviourally identical, so this is about intent. The assessment's own vocabulary distinguishes "a priority FIFO" from a FIFO, so the config speaks the same words. It also makes a surprise impossible, because on a queue created without priority a client sending `priority: 9` cannot silently jump the line. |
| 6 | **Two heaps, ready and delayed** | one heap plus a scan for eligible messages, or one heap with `visible_at` folded into the sort | A single sort key cannot express "not a candidate yet" without corrupting the ordering. Splitting eligibility from ordering keeps `policy.before` readable and makes promotion O(number actually promoted) rather than a scan. |
| 7 | **`visible_at` stored on the message** | a timer per delayed message | Timers do not survive a restart and would have to be reconstructed. A stored timestamp compared against the clock restores pending delays for free, which is why `TestDelayedMessageSurvivesRestart` needs no special handling. |
| 8 | **Sequence number for tie-breaks** | enqueue timestamp | Two messages enqueued in the same microsecond can share a timestamp, making FIFO ambiguous exactly under the load where it matters. The sequence is incremented under the queue lock, so it is unique and total by construction, and it is written into the log so ordering survives a restart without clock assumptions. |
| 9 | **One mutex per queue over log and index** | `RWMutex`, or separate locks for log and index | `RWMutex` only helps with readers that do not write, and both enqueue and dequeue write to both structures. Separate locks would let a goroutine observe an index that disagrees with the log. One lock across both makes "append, then update memory" atomic from outside. |
| 10 | **A CLI as the demo application** | a web UI, a dashboard | The assessment says "a **simple** application". The artefact under review is the queue, and the app only has to show it being used. A UI would signal a misread of the prompt. |
| 11 | **DLQ, auth, clustering, long polling, batch ops and metrics, all left out** | build a subset | Rule C, uniformly. Each is answered in the README's *Additional questions*, ranked by what I would do first. |

---

## Where I overrode the tool

**The scope questions.** Partway through, the model surfaced three items as open questions
for me. Whether priority should be a flag, whether to include compaction, and whether the
demo app should be a CLI or a UI. I rejected the framing. All three are answerable from the
assessment text, and treating them as preferences would have meant deciding the rest of the
build ad hoc too. We wrote down the A/B/C rule instead and applied it, which is where
decisions 4, 5 and 10 above come from. Item 4 changed outcome under the rule, since it looked like
a nice-to-have and turned out to be load-bearing for a requirement that is explicitly named.

**The language.** Go was my call, over the language I would have been faster to review, on
the grounds that stack match and a credible concurrency story were worth more here than my
own reading speed.

**The acknowledgement boundary.** Mine, and the one I would expect to be challenged on. The
instinct is that a queue without acks is incomplete. I took the narrower reading, because
the requirements do not ask for it and the assessment provides a section for exactly this
kind of "what would you add" answer. The trade is stated in the README rather than hidden,
which is the part I would defend, because at-most-once is a choice only if you say so.

---

## Verification

What was actually run, and what it caught.

| Check | Result |
|---|---|
| `gofmt -l .` | clean |
| `go vet ./...` | clean |
| `go test ./... -race` | 10 tests pass, including 500 messages across 8 concurrent consumers and a mixed producer/consumer run |
| `./scripts/demo.sh` | passes against a live server, covering every ordering mode, a delay, a `kill -9` and restart, and 4 concurrent workers reporting `total=200 unique=200 duplicates=0` |

Two things were caught by running rather than by reading, which is the reason the demo
script exists at all.

**A silent flag-parsing bug.** Go's `flag` package stops parsing at the first non-flag
argument, so `qctl create orders -order fifo -priority` left the flags unread and quietly
used the defaults. The first demo run failed on it. The fix is `takeArgs` in
[cmd/qctl/main.go](cmd/qctl/main.go), which peels off each subcommand's fixed positional
arguments before parsing the rest. As a side effect it also stops a message body beginning
with a dash from being mistaken for a flag.

**Two broken diagrams.** The first PNG export had a container label overlapped by the box
inside it, and the "no" branch routed on top of the `promote()` arrow so the two read as one
line. Both were found by looking at the rendered images, not the XML, and fixed in the
`.drawio` sources.

I also checked the tests themselves, by breaking six things on purpose and confirming which
test caught each one. Five were caught. Nothing caught the sixth, which was reversing the
write-ahead ordering in `Enqueue`, the step the durability argument rests on. A unit test
never dies between two statements. The table and what I concluded from it are in
[WALKTHROUGH.md §8](WALKTHROUGH.md#8-what-the-tests-are-for). Knowing which claims are tested
and which are only argued seemed more useful than a coverage percentage.

The tests are written as evidence for specific claims in the README rather than for
coverage. The mapping from claim to test is in
[WALKTHROUGH.md §8](WALKTHROUGH.md#8-what-the-tests-are-for). A claim in the README that no
test backs is one I would want to know about.

### Reproducing it

```bash
go test ./... -race     # the invariants
./scripts/demo.sh       # the same invariants against a live server, including kill -9
```

The demo uses a throwaway data directory and cleans up after itself.

package queue

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/juhokim/queuemaxxing/internal/wal"
)

// Compaction runs when the log is at least compactMinRecords long and at least
// half of it is garbage. Both conditions matter: the first stops us rewriting a
// tiny log constantly, the second keeps the amortised cost of a rewrite low.
const (
	compactMinRecords = 1024
	compactRatio      = 2
)

// Message is one item in a queue.
type Message struct {
	ID         string    `json:"id"`
	Seq        uint64    `json:"seq"`
	Body       string    `json:"body"`
	Priority   int       `json:"priority"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	VisibleAt  time.Time `json:"visible_at"`
}

// record is a message's on-disk form. Two kinds are written: "put" when a
// message is enqueued and "del" when it is delivered. Replaying them in order
// reconstructs the live set exactly.
type record struct {
	T    string `json:"t"`
	Seq  uint64 `json:"seq"`
	ID   string `json:"id,omitempty"`
	Body string `json:"body,omitempty"`
	Pri  int    `json:"pri,omitempty"`
	Enq  int64  `json:"enq,omitempty"` // unix milliseconds
	Vis  int64  `json:"vis,omitempty"` // unix milliseconds
}

// Stats is a point-in-time view of a queue's depth.
type Stats struct {
	Name       string `json:"name"`
	Policy     Policy `json:"policy"`
	Ready      int    `json:"ready"`   // deliverable right now
	Delayed    int    `json:"delayed"` // waiting for their delay to elapse
	LogRecords int    `json:"log_records"`
	LogBytes   int64  `json:"log_bytes"`
}

// Queue is a single durable queue: an ordering policy, an on-disk log, and the
// in-memory index rebuilt from that log at startup.
//
// One mutex guards both the index and the log. Every operation mutates both, so
// there are no read-only callers to separate out, and holding one lock across
// both is what makes "append to the log, then update memory" atomic from the
// outside -- no consumer can observe a message that is not yet durable.
type Queue struct {
	name   string
	policy Policy

	mu      sync.Mutex
	log     *wal.Log
	ready   *msgHeap // visible now, ordered by policy
	delayed *msgHeap // not yet visible, ordered by VisibleAt
	nextSeq uint64
	live    int
}

// Open opens the queue stored in dir, replaying its log to rebuild the index.
func Open(dir, name string, policy Policy) (*Queue, error) {
	log, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		return nil, err
	}
	q := &Queue{
		name:    name,
		policy:  policy,
		log:     log,
		ready:   &msgHeap{less: policy.before},
		delayed: &msgHeap{less: func(a, b *Message) bool { return a.VisibleAt.Before(b.VisibleAt) }},
	}

	// Replay applies puts and dels in write order, so the map ends up holding
	// exactly the messages that were live when the process stopped.
	alive := make(map[uint64]*Message)
	err = log.Replay(func(payload []byte) error {
		var r record
		if err := json.Unmarshal(payload, &r); err != nil {
			return err
		}
		switch r.T {
		case "put":
			alive[r.Seq] = &Message{
				ID:         r.ID,
				Seq:        r.Seq,
				Body:       r.Body,
				Priority:   r.Pri,
				EnqueuedAt: time.UnixMilli(r.Enq),
				VisibleAt:  time.UnixMilli(r.Vis),
			}
		case "del":
			delete(alive, r.Seq)
		default:
			return fmt.Errorf("unknown record type %q", r.T)
		}
		if r.Seq >= q.nextSeq {
			q.nextSeq = r.Seq + 1
		}
		return nil
	})
	if err != nil {
		log.Close()
		return nil, err
	}

	now := time.Now()
	for _, m := range alive {
		q.push(m, now)
	}
	q.live = len(alive)
	return q, nil
}

func (q *Queue) Name() string   { return q.name }
func (q *Queue) Policy() Policy { return q.policy }
func (q *Queue) Close() error   { q.mu.Lock(); defer q.mu.Unlock(); return q.log.Close() }

// Enqueue appends a message and returns once it is durable.
func (q *Queue) Enqueue(body string, priority int, delaySeconds *int) (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delay := q.policy.DelaySeconds
	if delaySeconds != nil {
		if *delaySeconds < 0 {
			return nil, fmt.Errorf("delay_seconds must not be negative")
		}
		delay = *delaySeconds
	}
	if !q.policy.Priority {
		priority = 0
	}

	now := time.Now()
	m := &Message{
		ID:         newID(),
		Seq:        q.nextSeq,
		Body:       body,
		Priority:   priority,
		EnqueuedAt: now,
		VisibleAt:  now.Add(time.Duration(delay) * time.Second),
	}

	// Write ahead: the log is updated first, so a crash between these two lines
	// loses nothing. If Append fails, no in-memory state has changed and the
	// caller gets an honest error.
	payload, err := json.Marshal(record{
		T: "put", Seq: m.Seq, ID: m.ID, Body: m.Body, Pri: m.Priority,
		Enq: m.EnqueuedAt.UnixMilli(), Vis: m.VisibleAt.UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	if err := q.log.Append(payload); err != nil {
		return nil, err
	}

	q.nextSeq++
	q.live++
	q.push(m, now)
	return m, nil
}

// Dequeue removes and returns the next deliverable message, or nil if none is
// deliverable right now. A message that exists but is still delayed counts as
// none: the queue is not empty, it is not ready.
func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promote(time.Now())
	if q.ready.Len() == 0 {
		return nil, nil
	}
	m := q.ready.items[0]

	// Same write-ahead discipline. The tombstone is durable before the message
	// leaves memory, so a crash here cannot redeliver a message we already
	// handed out.
	payload, err := json.Marshal(record{T: "del", Seq: m.Seq})
	if err != nil {
		return nil, err
	}
	if err := q.log.Append(payload); err != nil {
		return nil, err
	}

	heap.Pop(q.ready)
	q.live--
	if err := q.maybeCompact(); err != nil {
		return nil, err
	}
	return m, nil
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promote(time.Now())
	return Stats{
		Name:       q.name,
		Policy:     q.policy,
		Ready:      q.ready.Len(),
		Delayed:    q.delayed.Len(),
		LogRecords: q.log.Records(),
		LogBytes:   q.log.Size(),
	}
}

// push files a message into whichever heap matches its visibility.
func (q *Queue) push(m *Message, now time.Time) {
	if m.VisibleAt.After(now) {
		heap.Push(q.delayed, m)
	} else {
		heap.Push(q.ready, m)
	}
}

// promote moves every message whose delay has elapsed into the ready heap.
//
// This is where delay lives. Because the delayed heap is ordered by VisibleAt,
// the messages that are due are exactly a prefix of it, so this loop touches
// only the ones it actually moves -- no scanning, and no timers to keep alive
// across a restart.
func (q *Queue) promote(now time.Time) {
	for q.delayed.Len() > 0 && !q.delayed.items[0].VisibleAt.After(now) {
		heap.Push(q.ready, heap.Pop(q.delayed).(*Message))
	}
}

// maybeCompact rewrites the log from the live index when enough of the log has
// become garbage. Callers must hold q.mu.
func (q *Queue) maybeCompact() error {
	records := q.log.Records()
	if records < compactMinRecords || q.live*compactRatio > records {
		return nil
	}
	payloads := make([][]byte, 0, q.live)
	for _, h := range []*msgHeap{q.ready, q.delayed} {
		for _, m := range h.items {
			payload, err := json.Marshal(record{
				T: "put", Seq: m.Seq, ID: m.ID, Body: m.Body, Pri: m.Priority,
				Enq: m.EnqueuedAt.UnixMilli(), Vis: m.VisibleAt.UnixMilli(),
			})
			if err != nil {
				return err
			}
			payloads = append(payloads, payload)
		}
	}
	return q.log.Rewrite(payloads)
}

func newID() string {
	var b [16]byte
	rand.Read(b[:]) // crypto/rand.Read never returns an error; see its docs
	return hex.EncodeToString(b[:])
}

// msgHeap is a binary heap whose comparison is supplied at construction, which
// is how one type serves both the policy-ordered ready heap and the
// time-ordered delayed heap.
type msgHeap struct {
	items []*Message
	less  func(a, b *Message) bool
}

func (h *msgHeap) Len() int           { return len(h.items) }
func (h *msgHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h *msgHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *msgHeap) Push(x any)         { h.items = append(h.items, x.(*Message)) }

func (h *msgHeap) Pop() any {
	n := len(h.items)
	m := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	return m
}

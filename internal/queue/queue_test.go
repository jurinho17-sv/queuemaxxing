package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T, dir string, p Policy) *Queue {
	t.Helper()
	q, err := Open(dir, "test", p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return q
}

func send(t *testing.T, q *Queue, body string, priority int) {
	t.Helper()
	if _, err := q.Enqueue(body, priority, nil); err != nil {
		t.Fatalf("enqueue %s: %v", body, err)
	}
}

// drain pops until the queue reports nothing deliverable.
func drain(t *testing.T, q *Queue) []string {
	t.Helper()
	var out []string
	for {
		m, err := q.Dequeue()
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if m == nil {
			return out
		}
		out = append(out, m.Body)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The four policies the assessment names are one comparator with different
// settings, so one table covers all of them.
func TestPolicyOrdering(t *testing.T) {
	// Enqueued in this order, so sequence numbers run a=0 .. e=4.
	input := []struct {
		body     string
		priority int
	}{{"a", 1}, {"b", 5}, {"c", 1}, {"d", 5}, {"e", 3}}

	for _, tc := range []struct {
		name   string
		policy Policy
		want   []string
	}{
		{"fifo", Policy{Order: FIFO}, []string{"a", "b", "c", "d", "e"}},
		{"lifo", Policy{Order: LIFO}, []string{"e", "d", "c", "b", "a"}},
		{"priority fifo", Policy{Order: FIFO, Priority: true}, []string{"b", "d", "e", "a", "c"}},
		{"priority lifo", Policy{Order: LIFO, Priority: true}, []string{"d", "b", "e", "c", "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := open(t, t.TempDir(), tc.policy)
			defer q.Close()
			for _, in := range input {
				send(t, q, in.body, in.priority)
			}
			if got := drain(t, q); !equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Delay hides a message from the moment it is enqueued, not from the moment it
// is consumed. Until it becomes visible the queue reports nothing deliverable
// even though the message is stored.
func TestDelayHidesMessageUntilVisible(t *testing.T) {
	q := open(t, t.TempDir(), Policy{Order: FIFO, DelaySeconds: 0})
	defer q.Close()

	delay := 1
	if _, err := q.Enqueue("later", 0, &delay); err != nil {
		t.Fatal(err)
	}
	send(t, q, "now", 0)

	if s := q.Stats(); s.Ready != 1 || s.Delayed != 1 {
		t.Fatalf("ready=%d delayed=%d, want 1 and 1", s.Ready, s.Delayed)
	}
	if got := drain(t, q); !equal(got, []string{"now"}) {
		t.Fatalf("before the delay elapsed got %v, want [now]", got)
	}

	time.Sleep(1100 * time.Millisecond)
	if got := drain(t, q); !equal(got, []string{"later"}) {
		t.Fatalf("after the delay elapsed got %v, want [later]", got)
	}
}

// A per-message delay overrides the queue default in both directions.
func TestPerMessageDelayOverridesQueueDefault(t *testing.T) {
	q := open(t, t.TempDir(), Policy{Order: FIFO, DelaySeconds: 3600})
	defer q.Close()

	send(t, q, "inherits-the-hour", 0)
	zero := 0
	if _, err := q.Enqueue("immediate", 0, &zero); err != nil {
		t.Fatal(err)
	}
	if got := drain(t, q); !equal(got, []string{"immediate"}) {
		t.Errorf("got %v, want [immediate]", got)
	}
}

// Closing and reopening stands in for a crash: nothing is flushed on the way
// out, so whatever comes back was already durable.
func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	policy := Policy{Order: FIFO, Priority: true}

	q := open(t, dir, policy)
	for i := range 10 {
		send(t, q, fmt.Sprintf("msg-%d", i), i%3)
	}
	delivered := []string{}
	for range 3 {
		m, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		delivered = append(delivered, m.Body)
	}
	q.Close()

	reopened := open(t, dir, policy)
	defer reopened.Close()
	if s := reopened.Stats(); s.Ready != 7 {
		t.Fatalf("after restart ready=%d, want 7", s.Ready)
	}
	rest := drain(t, reopened)
	if len(rest) != 7 {
		t.Fatalf("recovered %d messages, want 7", len(rest))
	}
	// Nothing that was already delivered may come back.
	for _, got := range rest {
		for _, gone := range delivered {
			if got == gone {
				t.Errorf("%s was delivered before the restart and reappeared", got)
			}
		}
	}
}

// A message still inside its delay must come back still delayed, with its
// original visibility time -- the clock is stored, not a timer.
func TestDelayedMessageSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	policy := Policy{Order: FIFO}

	q := open(t, dir, policy)
	delay := 1
	if _, err := q.Enqueue("scheduled", 0, &delay); err != nil {
		t.Fatal(err)
	}
	q.Close()

	reopened := open(t, dir, policy)
	defer reopened.Close()
	if s := reopened.Stats(); s.Delayed != 1 || s.Ready != 0 {
		t.Fatalf("after restart ready=%d delayed=%d, want 0 and 1", s.Ready, s.Delayed)
	}
	time.Sleep(1100 * time.Millisecond)
	if got := drain(t, reopened); !equal(got, []string{"scheduled"}) {
		t.Errorf("got %v, want [scheduled]", got)
	}
}

// Run with -race. Every message must go to exactly one consumer.
func TestConcurrentConsumersNeverDuplicate(t *testing.T) {
	const (
		messages  = 500
		consumers = 8
	)
	q := open(t, t.TempDir(), Policy{Order: FIFO})
	defer q.Close()
	for i := range messages {
		send(t, q, fmt.Sprintf("msg-%d", i), 0)
	}

	var (
		mu   sync.Mutex
		seen = make(map[uint64]int)
		wg   sync.WaitGroup
	)
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				m, err := q.Dequeue()
				if err != nil || m == nil {
					return
				}
				mu.Lock()
				seen[m.Seq]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != messages {
		t.Errorf("consumed %d distinct messages, want %d", len(seen), messages)
	}
	for seq, n := range seen {
		if n != 1 {
			t.Errorf("seq %d was delivered %d times", seq, n)
		}
	}
}

// Producers and consumers running at the same time must not lose or duplicate
// anything either.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	const (
		producers = 4
		perProd   = 100
		consumers = 4
	)
	q := open(t, t.TempDir(), Policy{Order: FIFO, Priority: true})
	defer q.Close()

	var prodWG sync.WaitGroup
	for p := range producers {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for i := range perProd {
				if _, err := q.Enqueue(fmt.Sprintf("p%d-%d", p, i), i%5, nil); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	var (
		mu       sync.Mutex
		seen     = make(map[uint64]int)
		consWG   sync.WaitGroup
		done     = make(chan struct{})
		consumed = func(m *Message) {
			mu.Lock()
			seen[m.Seq]++
			mu.Unlock()
		}
	)
	for range consumers {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for {
				m, err := q.Dequeue()
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				if m != nil {
					consumed(m)
					continue
				}
				select {
				case <-done:
					return // producers finished and the queue came up empty
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	prodWG.Wait()
	close(done)
	consWG.Wait()

	// Anything the consumers did not get in the race to finish is still queued.
	for {
		m, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			break
		}
		consumed(m)
	}

	if want := producers * perProd; len(seen) != want {
		t.Errorf("accounted for %d messages, want %d", len(seen), want)
	}
	for seq, n := range seen {
		if n != 1 {
			t.Errorf("seq %d was delivered %d times", seq, n)
		}
	}
}

// Once most of the log is tombstones it is rewritten from the live index, so
// neither the file nor the startup replay grows without bound.
func TestCompactionShrinksLog(t *testing.T) {
	dir := t.TempDir()
	policy := Policy{Order: FIFO}
	q := open(t, dir, policy)

	const sent = 600
	for i := range sent {
		send(t, q, fmt.Sprintf("msg-%d", i), 0)
	}
	before := q.Stats().LogRecords

	// Compaction triggers once the log is long enough and at least half of it
	// is garbage: 600 puts + 424 dels = 1024 records for 176 live messages.
	for range 424 {
		if _, err := q.Dequeue(); err != nil {
			t.Fatal(err)
		}
	}
	s := q.Stats()
	if s.LogRecords >= before {
		t.Errorf("log holds %d records after compaction, want fewer than %d", s.LogRecords, before)
	}
	if s.LogRecords != s.Ready {
		t.Errorf("compacted log holds %d records for %d live messages", s.LogRecords, s.Ready)
	}
	q.Close()

	// The compacted log must still describe exactly the right messages.
	reopened := open(t, dir, policy)
	defer reopened.Close()
	got := drain(t, reopened)
	if len(got) != sent-424 {
		t.Fatalf("recovered %d messages, want %d", len(got), sent-424)
	}
	for i, body := range got {
		if want := fmt.Sprintf("msg-%d", 424+i); body != want {
			t.Fatalf("message %d = %s, want %s", i, body, want)
		}
	}
}

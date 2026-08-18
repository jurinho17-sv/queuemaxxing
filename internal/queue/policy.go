package queue

import (
	"errors"
	"fmt"
)

// Order is the tie-break rule applied to messages of equal priority.
type Order string

const (
	FIFO Order = "fifo" // oldest first
	LIFO Order = "lifo" // newest first
)

// Policy is the entire configurable personality of a queue.
//
// The assessment asks for a queue that can be "a delay, priority LIFO queue,
// or a priority FIFO". Those are not three implementations; they are three
// values of this struct:
//
//	{Order: LIFO, Priority: true, DelaySeconds: 30}  // a delay, priority LIFO queue
//	{Order: FIFO, Priority: true}                    // a priority FIFO
//	{Order: FIFO}                                    // a plain FIFO
type Policy struct {
	// Order decides which of two equally urgent messages goes first.
	Order Order `json:"order"`

	// Priority, when true, makes Priority the primary sort key. When false the
	// field is ignored on every message, so the queue is strictly Order.
	Priority bool `json:"priority"`

	// DelaySeconds hides a newly enqueued message for this long. This is the
	// per-queue default; a message may override it. Modelled on Amazon SQS
	// delay queues, where the delay starts when the message is *added*, unlike
	// a visibility timeout, which starts when it is *consumed*.
	DelaySeconds int `json:"delay_seconds"`
}

func (p Policy) Validate() error {
	switch p.Order {
	case FIFO, LIFO:
	default:
		return fmt.Errorf("order must be %q or %q, got %q", FIFO, LIFO, p.Order)
	}
	if p.DelaySeconds < 0 {
		return errors.New("delay_seconds must not be negative")
	}
	return nil
}

// before reports whether a should be delivered before b.
//
// This function is the whole ordering model. Priority is the primary key when
// the queue enables it; the sequence number breaks ties, ascending for FIFO and
// descending for LIFO. Delay does not appear here at all -- it decides *whether*
// a message is a candidate, not where it sorts. See Queue.promote.
func (p Policy) before(a, b *Message) bool {
	if p.Priority && a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if p.Order == FIFO {
		return a.Seq < b.Seq
	}
	return a.Seq > b.Seq
}

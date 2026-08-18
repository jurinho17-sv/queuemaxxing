// Command qctl is a small client application for the queue: create queues,
// send and pop messages, and run a pool of concurrent workers against one.
//
// It is the "simple application that can use and interact with the queue" the
// assessment asks for. It talks to the server over the same HTTP API any other
// client would use -- it imports nothing from internal/queue.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

const usage = `qctl -- a client for the queuemaxxing queue

usage: qctl [-addr URL] <command> [flags]

commands:
  create <name> [-order fifo|lifo] [-priority] [-delay N]   create a queue
  list                                                      list queues
  stats <name>                                              show one queue
  send <name> <body> [-priority N] [-delay N]               enqueue a message
  pop <name>                                                dequeue one message
  drain <name>                                              pop until empty
  worker <name> [-n N] [-for D]                             run N concurrent consumers
  delete <name>                                             delete a queue
`

func run(args []string) error {
	addr := "http://localhost:8080"
	if v := os.Getenv("QUEUE_ADDR"); v != "" {
		addr = v
	}
	// A bare -addr may precede the subcommand.
	for len(args) > 0 && args[0] == "-addr" {
		if len(args) < 2 {
			return errors.New("-addr needs a value")
		}
		addr, args = args[1], args[2:]
	}
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	c := &client{addr: addr, http: &http.Client{Timeout: 10 * time.Second}}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "create":
		return c.create(rest)
	case "list":
		return c.list()
	case "stats":
		return c.stats(rest)
	case "send":
		return c.send(rest)
	case "pop":
		return c.pop(rest)
	case "drain":
		return c.drain(rest)
	case "worker":
		return c.worker(rest)
	case "delete":
		return c.delete(rest)
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
}

type message struct {
	ID         string    `json:"id"`
	Seq        uint64    `json:"seq"`
	Body       string    `json:"body"`
	Priority   int       `json:"priority"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	VisibleAt  time.Time `json:"visible_at"`
}

type stats struct {
	Name   string `json:"name"`
	Policy struct {
		Order        string `json:"order"`
		Priority     bool   `json:"priority"`
		DelaySeconds int    `json:"delay_seconds"`
	} `json:"policy"`
	Ready      int   `json:"ready"`
	Delayed    int   `json:"delayed"`
	LogRecords int   `json:"log_records"`
	LogBytes   int64 `json:"log_bytes"`
}

// takeArgs splits off the n leading positional arguments and returns the rest
// for flag parsing.
//
// Go's flag package stops at the first non-flag argument, so "create q -order
// lifo" would leave the flags unparsed. Every subcommand here has a fixed
// number of positional arguments, so peeling them off first makes
// "qctl send q hello -priority 9" work, and keeps a body that starts with a
// dash from being mistaken for a flag.
func takeArgs(args []string, n int, usage string) ([]string, []string, error) {
	if len(args) < n {
		return nil, nil, errors.New(usage)
	}
	return args[:n], args[n:], nil
}

func (c *client) create(args []string) error {
	pos, rest, err := takeArgs(args, 1, "usage: qctl create <name> [-order fifo|lifo] [-priority] [-delay N]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	order := fs.String("order", "fifo", "fifo or lifo")
	priority := fs.Bool("priority", false, "sort by priority before order")
	delay := fs.Int("delay", 0, "default delay in seconds")
	fs.Parse(rest)
	body := map[string]any{
		"name": pos[0], "order": *order,
		"priority": *priority, "delay_seconds": *delay,
	}
	var s stats
	if _, err := c.do("POST", "/queues", body, &s); err != nil {
		return err
	}
	fmt.Printf("created %s (order=%s priority=%v delay=%ds)\n",
		s.Name, s.Policy.Order, s.Policy.Priority, s.Policy.DelaySeconds)
	return nil
}

func (c *client) list() error {
	var out []stats
	if _, err := c.do("GET", "/queues", nil, &out); err != nil {
		return err
	}
	if len(out) == 0 {
		fmt.Println("no queues")
		return nil
	}
	fmt.Printf("%-20s %-6s %-9s %-7s %7s %9s\n", "NAME", "ORDER", "PRIORITY", "DELAY", "READY", "DELAYED")
	for _, s := range out {
		fmt.Printf("%-20s %-6s %-9v %-7s %7d %9d\n",
			s.Name, s.Policy.Order, s.Policy.Priority,
			fmt.Sprintf("%ds", s.Policy.DelaySeconds), s.Ready, s.Delayed)
	}
	return nil
}

func (c *client) stats(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: qctl stats <name>")
	}
	var s stats
	if _, err := c.do("GET", "/queues/"+args[0], nil, &s); err != nil {
		return err
	}
	fmt.Printf("%s: order=%s priority=%v delay=%ds ready=%d delayed=%d log=%d records/%d bytes\n",
		s.Name, s.Policy.Order, s.Policy.Priority, s.Policy.DelaySeconds,
		s.Ready, s.Delayed, s.LogRecords, s.LogBytes)
	return nil
}

func (c *client) send(args []string) error {
	pos, rest, err := takeArgs(args, 2, "usage: qctl send <name> <body> [-priority N] [-delay N]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	priority := fs.Int("priority", 0, "higher goes first (needs a priority queue)")
	delay := fs.Int("delay", -1, "override the queue delay, in seconds")
	fs.Parse(rest)
	body := map[string]any{"body": pos[1], "priority": *priority}
	if *delay >= 0 {
		body["delay_seconds"] = *delay
	}
	var m message
	if _, err := c.do("POST", "/queues/"+pos[0]+"/messages", body, &m); err != nil {
		return err
	}
	fmt.Printf("sent seq=%d priority=%d visible_at=%s\n",
		m.Seq, m.Priority, m.VisibleAt.Format(time.TimeOnly))
	return nil
}

func (c *client) pop(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: qctl pop <name>")
	}
	m, ok, err := c.popOne(args[0])
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println("(nothing ready)")
		return nil
	}
	fmt.Printf("seq=%-4d priority=%-3d %s\n", m.Seq, m.Priority, m.Body)
	return nil
}

func (c *client) drain(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: qctl drain <name>")
	}
	n := 0
	for {
		m, ok, err := c.popOne(args[0])
		if err != nil {
			return err
		}
		if !ok {
			fmt.Printf("(drained %d)\n", n)
			return nil
		}
		fmt.Printf("seq=%-4d priority=%-3d %s\n", m.Seq, m.Priority, m.Body)
		n++
	}
}

// worker runs n consumers against one queue at the same time and reports what
// each of them got. Because every message is recorded with its sequence number,
// the summary line doubles as a check that concurrent consumers never receive
// the same message twice.
func (c *client) worker(args []string) error {
	pos, rest, err := takeArgs(args, 1, "usage: qctl worker <name> [-n N] [-for D]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	n := fs.Int("n", 3, "number of concurrent consumers")
	dur := fs.Duration("for", 3*time.Second, "how long to keep consuming")
	quiet := fs.Bool("quiet", false, "only print the summary")
	fs.Parse(rest)
	name := pos[0]

	var (
		mu       sync.Mutex
		seen     = make(map[uint64]int)
		dupes    int
		perWorks = make([]int, *n)
		deadline = time.Now().Add(*dur)
		wg       sync.WaitGroup
	)
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				m, ok, err := c.popOne(name)
				if err != nil {
					fmt.Fprintln(os.Stderr, "worker", id, err)
					return
				}
				if !ok {
					time.Sleep(25 * time.Millisecond)
					continue
				}
				mu.Lock()
				seen[m.Seq]++
				if seen[m.Seq] > 1 {
					dupes++
				}
				perWorks[id]++
				mu.Unlock()
				if !*quiet {
					fmt.Printf("worker-%d  seq=%-4d priority=%-3d %s\n", id, m.Seq, m.Priority, m.Body)
				}
			}
		}(i)
	}
	wg.Wait()

	total := 0
	for i, got := range perWorks {
		fmt.Printf("worker-%d consumed %d\n", i, got)
		total += got
	}
	fmt.Printf("total=%d unique=%d duplicates=%d\n", total, len(seen), dupes)
	if dupes > 0 {
		return fmt.Errorf("%d messages were delivered more than once", dupes)
	}
	return nil
}

func (c *client) delete(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: qctl delete <name>")
	}
	if _, err := c.do("DELETE", "/queues/"+args[0], nil, nil); err != nil {
		return err
	}
	fmt.Printf("deleted %s\n", args[0])
	return nil
}

type client struct {
	addr string
	http *http.Client
}

// popOne returns the next message, or ok=false when the queue has nothing
// deliverable right now (HTTP 204).
func (c *client) popOne(name string) (message, bool, error) {
	var m message
	code, err := c.do("POST", "/queues/"+name+"/messages/pop", nil, &m)
	if err != nil {
		return m, false, err
	}
	return m, code == http.StatusOK, nil
}

func (c *client) do(method, path string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, c.addr+path, body)
	if err != nil {
		return 0, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return resp.StatusCode, fmt.Errorf("%s %s: %s", method, path, e.Error)
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

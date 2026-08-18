package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
)

var (
	ErrNotFound = errors.New("queue not found")
	ErrExists   = errors.New("queue already exists")

	// A queue name becomes a directory name, so it is restricted rather than
	// escaped: no dots, no separators, nothing that could climb out of the data
	// directory.
	nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
)

// Broker owns every queue and the directory they live in.
//
// Its lock protects only the name -> queue map. Once a caller has a *Queue it
// works against that queue's own mutex, so traffic on different queues runs in
// parallel and never contends here.
type Broker struct {
	dir    string
	mu     sync.RWMutex
	queues map[string]*Queue
}

// NewBroker opens dir and reopens every queue it finds, replaying each log.
func NewBroker(dir string) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Broker{dir: dir, queues: make(map[string]*Queue)}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		qdir := filepath.Join(dir, e.Name())
		policy, err := readMeta(qdir)
		if err != nil {
			if os.IsNotExist(err) {
				continue // not one of ours
			}
			return nil, fmt.Errorf("queue %q: %w", e.Name(), err)
		}
		q, err := Open(qdir, e.Name(), policy)
		if err != nil {
			return nil, fmt.Errorf("queue %q: %w", e.Name(), err)
		}
		b.queues[e.Name()] = q
	}
	return b, nil
}

// Create makes a new queue and persists its policy before returning.
func (b *Broker) Create(name string, policy Policy) (*Queue, error) {
	if !nameRE.MatchString(name) {
		return nil, fmt.Errorf("invalid queue name %q: must match %s", name, nameRE)
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[name]; ok {
		return nil, ErrExists
	}

	qdir := filepath.Join(b.dir, name)
	if err := os.MkdirAll(qdir, 0o755); err != nil {
		return nil, err
	}
	if err := writeMeta(qdir, policy); err != nil {
		return nil, err
	}
	q, err := Open(qdir, name, policy)
	if err != nil {
		return nil, err
	}
	b.queues[name] = q
	return q, nil
}

func (b *Broker) Get(name string) (*Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.queues[name]
	if !ok {
		return nil, ErrNotFound
	}
	return q, nil
}

// Delete closes the queue and removes its directory, log included.
func (b *Broker) Delete(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		return ErrNotFound
	}
	delete(b.queues, name)
	if err := q.Close(); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(b.dir, name))
}

// List returns stats for every queue, ordered by name so output is stable.
func (b *Broker) List() []Stats {
	b.mu.RLock()
	queues := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		queues = append(queues, q)
	}
	b.mu.RUnlock()

	out := make([]Stats, 0, len(queues))
	for _, q := range queues {
		out = append(out, q.Stats())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, q := range b.queues {
		if err := q.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.queues = make(map[string]*Queue)
	return firstErr
}

func metaPath(qdir string) string { return filepath.Join(qdir, "meta.json") }

func readMeta(qdir string) (Policy, error) {
	var p Policy
	data, err := os.ReadFile(metaPath(qdir))
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, err
	}
	return p, p.Validate()
}

// writeMeta persists a policy atomically: write a temporary file, fsync it,
// then rename over the target. A reader can never see a half-written policy.
func writeMeta(qdir string, p Policy) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := metaPath(qdir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, metaPath(qdir)); err != nil {
		return err
	}
	d, err := os.Open(qdir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

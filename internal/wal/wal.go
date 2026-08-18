// Package wal implements a durable, append-only record log.
//
// This is the only storage primitive in the project. The assessment forbids
// delegating storage to a separate queue or database, so everything above this
// package -- ordering, priority, delay, recovery -- is built on the two
// guarantees the log provides:
//
//  1. A record that Append returns nil for is on disk, and survives a crash.
//  2. Replay hands back exactly those records, in the order they were written.
//
// Records are framed on disk as:
//
//	[4B payload length, big endian][4B CRC32 of payload][payload]
//
// The length lets Replay find record boundaries; the checksum lets it tell a
// complete record from one that was half-written when the process died.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	headerSize = 8

	// maxRecordSize bounds how much we will allocate from a length prefix we
	// read off disk. Without it, a corrupt prefix could ask for gigabytes.
	maxRecordSize = 8 << 20
)

// ErrReplayed is returned when Replay is called more than once, or after the
// log has already been appended to.
var ErrReplayed = errors.New("wal: Replay must be called once, before the first Append")

// Log is an append-only file. It is not safe for concurrent use; callers hold
// their own lock (see queue.Queue, which serialises the log with its index).
type Log struct {
	path     string
	f        *os.File
	size     int64
	records  int
	replayed bool
}

// Open opens (or creates) the log at path. The caller must call Replay before
// the first Append, so that the write offset lands past any existing records.
func Open(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	// fsync the directory so the file's existence -- not just its contents --
	// survives a power loss.
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &Log{path: path, f: f}, nil
}

// Replay reads the log from the beginning and calls fn once per intact record.
//
// It stops at the first record that is incomplete or fails its checksum. That
// can only be the last record -- a crash can tear the write in flight, but not
// one that already returned from fsync -- so everything from that point on is
// truncated, leaving the log at a clean boundary for the next Append.
func (l *Log) Replay(fn func(payload []byte) error) error {
	if l.replayed {
		return ErrReplayed
	}
	l.replayed = true

	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(l.f)

	var (
		offset int64
		count  int
		header [headerSize]byte
	)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			break // clean EOF, or a torn header
		}
		length := binary.BigEndian.Uint32(header[0:4])
		if length == 0 || length > maxRecordSize {
			break // the length prefix itself is garbage
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // the record was cut short
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
			break // the bytes are there but they are not what was written
		}
		if err := fn(payload); err != nil {
			return fmt.Errorf("wal: apply record at offset %d: %w", offset, err)
		}
		offset += headerSize + int64(length)
		count++
	}

	if err := l.f.Truncate(offset); err != nil {
		return err
	}
	if _, err := l.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	l.size, l.records = offset, count
	return l.f.Sync()
}

// Append writes one record and fsyncs it. It returns only once the bytes are
// durable, which is what lets the HTTP layer answer 200 and mean it.
func (l *Log) Append(payload []byte) error {
	if len(payload) == 0 || len(payload) > maxRecordSize {
		return fmt.Errorf("wal: record size %d out of range", len(payload))
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerSize:], payload)

	if _, err := l.f.Write(buf); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.size += int64(len(buf))
	l.records++
	return nil
}

// Rewrite replaces the log's entire contents with payloads.
//
// This is compaction. A queue deletes every message it delivers, so a log that
// is only ever appended to grows without bound -- and since Replay walks the
// whole log, startup time would grow with it. Rewriting drops the records that
// no longer describe live state.
//
// The swap is crash-safe: the new log is written and fsynced to a temporary
// file first, and rename(2) is atomic, so a crash at any point leaves either
// the old complete log or the new complete log, never a mixture.
func (l *Log) Rewrite(payloads [][]byte) error {
	tmp := l.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var size int64
	header := make([]byte, headerSize)
	for _, p := range payloads {
		binary.BigEndian.PutUint32(header[0:4], uint32(len(p)))
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(p))
		if _, err := w.Write(header); err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write(p); err != nil {
			f.Close()
			return err
		}
		size += headerSize + int64(len(p))
	}
	if err := w.Flush(); err != nil {
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
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(l.path)); err != nil {
		return err
	}

	// Point the handle at the file we just swapped in.
	if err := l.f.Close(); err != nil {
		return err
	}
	nf, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := nf.Seek(size, io.SeekStart); err != nil {
		nf.Close()
		return err
	}
	l.f, l.size, l.records = nf, size, len(payloads)
	return nil
}

// Records is the number of records currently in the log, live or superseded.
func (l *Log) Records() int { return l.records }

// Size is the log's size on disk in bytes.
func (l *Log) Size() int64 { return l.size }

func (l *Log) Close() error { return l.f.Close() }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

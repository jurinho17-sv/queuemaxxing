package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, dir string) (*Log, [][]byte) {
	t.Helper()
	l, err := Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var got [][]byte
	if err := l.Replay(func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return l, got
}

func TestAppendSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	l, got := openTemp(t, dir)
	if len(got) != 0 {
		t.Fatalf("new log replayed %d records, want 0", len(got))
	}
	for i := range 5 {
		if err := l.Append([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	l.Close()

	l2, got := openTemp(t, dir)
	defer l2.Close()
	if len(got) != 5 {
		t.Fatalf("replayed %d records, want 5", len(got))
	}
	for i, p := range got {
		if want := fmt.Sprintf("record-%d", i); string(p) != want {
			t.Errorf("record %d = %q, want %q", i, p, want)
		}
	}
}

// A crash can leave a partly written record at the end of the log. Replay must
// discard it and leave the file at the last good boundary, so the next append
// does not build on top of garbage.
func TestReplayDiscardsTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	l, _ := openTemp(t, dir)
	for i := range 3 {
		if err := l.Append([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	good := l.Size()
	l.Close()

	// Simulate the crash: a header claiming more bytes than actually landed.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 64, 1, 2, 3, 4, 'h', 'a', 'l', 'f'})
	f.Close()

	l2, got := openTemp(t, dir)
	defer l2.Close()
	if len(got) != 3 {
		t.Fatalf("replayed %d records, want 3", len(got))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != good {
		t.Errorf("log is %d bytes after replay, want it truncated to %d", info.Size(), good)
	}

	// The log must still be usable.
	if err := l2.Append([]byte("record-3")); err != nil {
		t.Fatalf("append after truncation: %v", err)
	}
	l2.Close()
	l3, got := openTemp(t, dir)
	defer l3.Close()
	if len(got) != 4 || string(got[3]) != "record-3" {
		t.Errorf("after reopen got %d records, last %q", len(got), got[len(got)-1])
	}
}

// A corrupt record in the middle -- bit rot rather than a torn write -- must not
// be handed to the caller as if it were valid.
func TestReplayStopsAtBadChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	l, _ := openTemp(t, dir)
	l.Append([]byte("keep-me"))
	l.Append([]byte("corrupt"))
	l.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[headerSize+len("keep-me")+headerSize] ^= 0xFF // flip a bit in record 2
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	l2, got := openTemp(t, dir)
	defer l2.Close()
	if len(got) != 1 || string(got[0]) != "keep-me" {
		t.Fatalf("got %d records %q, want just [keep-me]", len(got), got)
	}
}

func TestRewriteReplacesContents(t *testing.T) {
	dir := t.TempDir()
	l, _ := openTemp(t, dir)
	for i := range 10 {
		l.Append([]byte(fmt.Sprintf("old-%d", i)))
	}
	if err := l.Rewrite([][]byte{[]byte("new-0"), []byte("new-1")}); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if l.Records() != 2 {
		t.Errorf("Records() = %d after rewrite, want 2", l.Records())
	}
	// Appending after a rewrite must land after the rewritten records.
	if err := l.Append([]byte("new-2")); err != nil {
		t.Fatalf("append after rewrite: %v", err)
	}
	l.Close()

	l2, got := openTemp(t, dir)
	defer l2.Close()
	want := []string{"new-0", "new-1", "new-2"}
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

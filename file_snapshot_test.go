// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"encoding/json"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestFileSnapshotStoreImpl(t *testing.T) {
	var impl interface{} = &FileSnapshotStore{}
	if _, ok := impl.(SnapshotStore); !ok {
		t.Fatalf("FileSnapshotStore not a SnapshotStore")
	}
}

func TestFileSnapshotSinkImpl(t *testing.T) {
	var impl interface{} = &FileSnapshotSink{}
	if _, ok := impl.(SnapshotSink); !ok {
		t.Fatalf("FileSnapshotSink not a SnapshotSink")
	}
}

func TestFileSS_CreateSnapshotMissingParentDir(t *testing.T) {
	parent, err := os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(parent) }()

	dir, err := os.MkdirTemp(parent, "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_ = os.RemoveAll(parent)
	_, trans := NewInmemTransport(NewInmemAddr())
	_, err = snap.Create(SnapshotVersionMax, 10, 3, Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("should not fail when using non existing parent")
	}
}

func TestFileSS_CreateSnapshot(t *testing.T) {
	// Create a test dir
	dir, err := os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check no snapshots
	snaps, err := snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}

	// Create a new sink
	var configuration Configuration
	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Voter,
		ID:       ServerID("my id"),
		Address:  ServerAddress("over here"),
	})
	_, trans := NewInmemTransport(NewInmemAddr())
	sink, err := snap.Create(SnapshotVersionMax, 10, 3, configuration, 2, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The sink is not done, should not be in a list!
	snaps, err = snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}

	// Write to the sink
	_, err = sink.Write([]byte("first\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_, err = sink.Write([]byte("second\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Done!
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should have a snapshot!
	snaps, err = snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expect a snapshots: %v", snaps)
	}

	// Check the latest
	latest := snaps[0]
	if latest.Index != 10 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Term != 3 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if !reflect.DeepEqual(latest.Configuration, configuration) {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.ConfigurationIndex != 2 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Size != 13 {
		t.Fatalf("bad snapshot: %v", *latest)
	}

	// Read the snapshot
	_, r, err := snap.Open(latest.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Read out everything
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure a match
	if !bytes.Equal(buf.Bytes(), []byte("first\nsecond\n")) {
		t.Fatalf("content mismatch")
	}
}

func TestFileSS_CreateSnapshotVersion(t *testing.T) {
	dir := t.TempDir()
	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for _, version := range []SnapshotVersion{0, SnapshotVersionMax} {
		_, trans := NewInmemTransport(NewInmemAddr())
		sink, err := snap.Create(version, uint64(version+10), 3, Configuration{}, 0, trans)
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if _, err := sink.Write([]byte("state")); err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("version %d: %v", version, err)
		}

		metaPath := filepath.Join(dir, snapPath, sink.ID(), metaFilePath)
		metaData, err := os.ReadFile(metaPath)
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		var onDisk map[string]interface{}
		if err := json.Unmarshal(metaData, &onDisk); err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if onDisk["Version"] != float64(version) {
			t.Fatalf("version %d: metadata has version %v", version, onDisk["Version"])
		}

		meta, source, err := snap.Open(sink.ID())
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if meta.Version != version {
			t.Fatalf("version %d: got version %d", version, meta.Version)
		}
		_ = source.Close()
	}
}

func TestFileSS_CreateUnsupportedSnapshotVersion(t *testing.T) {
	dir := t.TempDir()
	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	_, trans := NewInmemTransport(NewInmemAddr())
	version := SnapshotVersion(SnapshotVersionMax + 1)
	_, err = snap.Create(version, 10, 3, Configuration{}, 0, trans)
	if err == nil {
		t.Fatalf("expected unsupported snapshot version error")
	}
	if !strings.Contains(err.Error(), "unsupported snapshot version 2") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFileSS_LegacySnapshotWithoutVersion(t *testing.T) {
	dir := t.TempDir()
	const id = "1-10-123"
	state := []byte("legacy state")

	meta := struct {
		ID    string
		Index uint64
		Term  uint64
		Peers []byte
		Size  int64
		CRC   []byte
	}{
		ID:    id,
		Index: 10,
		Term:  1,
		Size:  int64(len(state)),
		CRC:   snapshotCRCForTest(state),
	}
	writeFileSnapshotForTest(t, dir, id, meta, state)

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	snaps, err := snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected one snapshot, got %d", len(snaps))
	}
	if snaps[0].Version != 0 {
		t.Fatalf("expected legacy version 0, got %d", snaps[0].Version)
	}

	opened, source, err := snap.Open(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = source.Close() }()
	if opened.Version != 0 {
		t.Fatalf("expected legacy version 0, got %d", opened.Version)
	}
}

func TestFileSS_UnsupportedSnapshotVersion(t *testing.T) {
	dir := t.TempDir()
	const id = "2-10-123"
	state := []byte("future state")

	meta := struct {
		Version int
		ID      string
		Index   uint64
		Term    uint64
		Peers   []byte
		Size    int64
		CRC     []byte
	}{
		Version: 2,
		ID:      id,
		Index:   10,
		Term:    2,
		Size:    int64(len(state)),
		CRC:     snapshotCRCForTest(state),
	}
	writeFileSnapshotForTest(t, dir, id, meta, state)

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err := snap.List(); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 2") {
		t.Fatalf("unexpected list error: %v", err)
	}
	if _, _, err := snap.Open(id); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 2") {
		t.Fatalf("unexpected open error: %v", err)
	}
}

func TestFileSS_CancelSnapshot(t *testing.T) {
	// Create a test dir
	dir, err := os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Create a new sink
	_, trans := NewInmemTransport(NewInmemAddr())
	sink, err := snap.Create(SnapshotVersionMax, 10, 3, Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Cancel the snapshot! Should delete
	err = sink.Cancel()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The sink is canceled, should not be in a list!
	snaps, err := snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}
}

func TestFileSS_Retention(t *testing.T) {
	var err error
	// Create a test dir
	var dir string
	dir, err = os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var snap *FileSnapshotStore
	snap, err = NewFileSnapshotStoreWithLogger(dir, 2, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Create a few snapshots
	_, trans := NewInmemTransport(NewInmemAddr())
	for i := 10; i < 15; i++ {
		var sink SnapshotSink
		sink, err = snap.Create(SnapshotVersionMax, uint64(i), 3, Configuration{}, 0, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		err = sink.Close()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Should only have 2 listed!
	var snaps []*SnapshotMeta
	snaps, err = snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expect 2 snapshots: %v", snaps)
	}

	// Check they are the latest
	if snaps[0].Index != 14 {
		t.Fatalf("bad snap: %#v", *snaps[0])
	}
	if snaps[1].Index != 13 {
		t.Fatalf("bad snap: %#v", *snaps[1])
	}
}

func TestFileSS_BadPerm(t *testing.T) {
	var err error
	if runtime.GOOS == "windows" {
		t.Skip("skipping file permission test on windows")
	}

	// Create a temp dir
	var dir1 string
	dir1, err = os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer func() { _ = os.RemoveAll(dir1) }()

	// Create a sub dir and remove all permissions
	var dir2 string
	dir2, err = os.MkdirTemp(dir1, "badperm")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err = os.Chmod(dir2, 0o00); err != nil {
		t.Fatalf("err: %s", err)
	}
	defer func() { _ = os.Chmod(dir2, 0777) }() // Set perms back for delete

	// Should fail
	if _, err = NewFileSnapshotStore(dir2, 3, nil); err == nil {
		t.Fatalf("should fail to use dir with bad perms")
	}
}

func TestFileSS_MissingParentDir(t *testing.T) {
	parent, err := os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(parent) }()

	dir, err := os.MkdirTemp(parent, "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	_ = os.RemoveAll(parent)
	_, err = NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		t.Fatalf("should not fail when using non existing parent")
	}
}

func TestFileSS_Ordering(t *testing.T) {
	// Create a test dir
	dir, err := os.MkdirTemp("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Create a new sink
	_, trans := NewInmemTransport(NewInmemAddr())
	sink, err := snap.Create(SnapshotVersionMax, 130350, 5, Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	sink, err = snap.Create(SnapshotVersionMax, 204917, 36, Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should only have 2 listed!
	snaps, err := snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expect 2 snapshots: %v", snaps)
	}

	// Check they are ordered
	if snaps[0].Term != 36 {
		t.Fatalf("bad snap: %#v", *snaps[0])
	}
	if snaps[1].Term != 5 {
		t.Fatalf("bad snap: %#v", *snaps[1])
	}
}

func writeFileSnapshotForTest(t *testing.T, base, id string, meta interface{}, state []byte) {
	t.Helper()

	dir := filepath.Join(base, snapPath, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("err: %v", err)
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, metaFilePath), metaData, 0o644); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, stateFilePath), state, 0o644); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func snapshotCRCForTest(state []byte) []byte {
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))
	_, _ = stateHash.Write(state)
	return stateHash.Sum(nil)
}

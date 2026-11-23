// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestRaft_SnapshotRecoveryKeepsLogsForIncrementalCatchUp(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	conf.SnapshotThreshold = 1 << 40
	conf.SnapshotInterval = time.Hour

	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	behind := c.Followers()[0]
	c.Disconnect(behind.localAddr)

	var future Future
	for i := 0; i < 10000; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("before-snapshot-%d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("failed to apply logs: %v", err)
	}
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("failed to snapshot leader: %v", err)
	}

	snaps, err := leader.snapshots.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %v", err)
	}
	if len(snaps) == 0 {
		t.Fatal("expected leader snapshot")
	}
	snapIdx := snaps[0].Index
	firstIdx, _ := leader.logs.FirstIndex()
	behindIdx, _ := behind.logs.LastIndex()
	if behindIdx >= firstIdx {
		t.Fatalf("follower is not far enough behind to need a snapshot: follower=%d first=%d", behindIdx, firstIdx)
	}

	// Hold the snapshot transfer open while more logs are appended and the
	// leader snapshots again.
	releaseSnapshot := make(chan struct{})
	behind.trans.(*InmemTransport).DelayInstallSnapshot(releaseSnapshot)
	c.FullyConnect()

	deadline := time.Now().Add(5 * time.Second)
	for leader.lowestRecoveringPeerIndex() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("leader did not start recovering the follower")
		}
		time.Sleep(time.Millisecond)
	}

	for i := 0; i < 10000; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("during-snapshot-%d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("failed to apply logs during snapshot: %v", err)
	}
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("failed to snapshot leader during recovery: %v", err)
	}

	// The entries appended while the snapshot was transferring must still be
	// available so the follower can catch up without another full snapshot.
	var log Log
	if err := leader.logs.GetLog(snapIdx, &log); err != nil {
		t.Fatalf("snapshot index was compacted during recovery: %v", err)
	}

	close(releaseSnapshot)
	c.EnsureSame(t)

	lastSnapshot, _ := behind.getLastSnapshot()
	if lastSnapshot != snapIdx {
		t.Fatalf("follower installed another snapshot: got %d want %d", lastSnapshot, snapIdx)
	}

	deadline = time.Now().Add(5 * time.Second)
	for leader.lowestRecoveringPeerIndex() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("leader kept the follower in recovery mode after catch-up")
		}
		time.Sleep(time.Millisecond)
	}

	// Once caught up, normal trailing-log compaction should resume.
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("failed to snapshot after recovery: %v", err)
	}
	firstIdx, _ = leader.logs.FirstIndex()
	lastIdx, _ := leader.logs.LastIndex()
	if firstIdx <= snapIdx {
		t.Fatalf("logs before the recovered snapshot were not compacted: first=%d snapshot=%d", firstIdx, snapIdx)
	}
	if lastIdx-firstIdx+1 < conf.TrailingLogs {
		t.Fatalf("leader did not retain trailing logs: first=%d last=%d trailing=%d", firstIdx, lastIdx, conf.TrailingLogs)
	}
}

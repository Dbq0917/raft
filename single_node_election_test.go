// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitForLeadership blocks until r reports leadership through LeaderCh() or
// fails the test after a bounded wait. It returns the elapsed time so callers
// can record how long election took as supporting data.
func waitForLeadership(t *testing.T, r *Raft, timeout time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case isLeader := <-r.LeaderCh():
		require.True(t, isLeader, "node did not become leader")
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for %s to elect itself", timeout, r.localID)
	}
	elapsed := time.Since(start)
	require.Equal(t, Leader, r.State(), "node should be leader after winning the election")
	return elapsed
}

// restartSingleNode shuts the single-node raft instance down and restarts it
// over the same durable stores with a fresh transport, mirroring an edge
// deployment process restart.
func restartSingleNode(t *testing.T, c *cluster, index int) *Raft {
	t.Helper()
	old := c.rafts[index]
	require.NoError(t, old.Shutdown().Error())

	_, trans := NewInmemTransport(old.trans.LocalAddr())
	cfg := old.config()
	restarted, err := NewRaft(&cfg, old.fsm, old.logs, old.stable, old.snapshots, trans)
	require.NoError(t, err)
	c.rafts[index] = restarted
	return restarted
}

// TestRaft_SingleNode_RestartElectsSelfAndWrites is the acceptance test for the
// edge single-node deployment: after a full process restart the node must
// elect itself immediately, recognize its own vote, and keep accepting writes.
func TestRaft_SingleNode_RestartElectsSelfAndWrites(t *testing.T) {
	cases := []struct {
		name            string
		preVoteDisabled bool
	}{
		{"pre-vote", false},
		{"no-pre-vote", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := inmemConfig(t)
			conf.PreVoteDisabled = tc.preVoteDisabled
			c := MakeCluster(1, t, conf)
			defer c.Close()

			leader := c.Leader()
			before := leader.Apply([]byte("before-restart"), conf.HeartbeatTimeout)
			require.NoError(t, before.Error())
			require.NotZero(t, before.Index())

			restarted := restartSingleNode(t, c, 0)
			elapsed := waitForLeadership(t, restarted, conf.HeartbeatTimeout*10)
			t.Logf("single node elected itself in %s (preVoteDisabled=%v)", elapsed, tc.preVoteDisabled)

			// A single voter forms a quorum of one, so its own persisted
			// vote must be enough to win without contacting any peer.
			after := restarted.Apply([]byte("after-restart"), conf.HeartbeatTimeout)
			require.NoError(t, after.Error(), "restarted single node must keep accepting writes")
			require.Greater(t, after.Index(), before.Index(), "post-restart write must extend the log")

			logs := getMockFSM(c.fsms[0]).Logs()
			require.Contains(t, logs, []byte("before-restart"))
			require.Contains(t, logs, []byte("after-restart"))
		})
	}
}

// TestRaft_SingleNode_RestartAfterSnapshot makes sure a restarted single node
// still elects itself when the original configuration entry has been compacted
// into a snapshot, i.e. configuration is recovered from snapshot state rather
// than a scanned log entry.
func TestRaft_SingleNode_RestartAfterSnapshot(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 1
	c := MakeCluster(1, t, conf)
	defer c.Close()

	leader := c.Leader()
	var last Future
	for i := 0; i < 50; i++ {
		last = leader.Apply([]byte("bulk"), conf.HeartbeatTimeout)
	}
	require.NoError(t, last.Error())
	require.NoError(t, leader.Snapshot().Error())

	firstIndex, err := leader.logs.FirstIndex()
	require.NoError(t, err)
	require.Greater(t, firstIndex, uint64(1), "configuration entry should be compacted into the snapshot")

	restarted := restartSingleNode(t, c, 0)
	elapsed := waitForLeadership(t, restarted, conf.HeartbeatTimeout*10)
	t.Logf("single node elected itself after snapshot restore in %s", elapsed)

	after := restarted.Apply([]byte("after-snapshot-restart"), conf.HeartbeatTimeout)
	require.NoError(t, after.Error())
}

// TestRaft_SingleNode_AddSecondNodeKeepsElectionBehavior verifies the cluster
// join flow is unchanged once a second voter is added to a working single-node
// cluster: the original leader stays leader, the joined node follows, and the
// two-node quorum replicates and commits writes.
func TestRaft_SingleNode_AddSecondNodeKeepsElectionBehavior(t *testing.T) {
	cases := []struct {
		name            string
		preVoteDisabled bool
	}{
		{"pre-vote", false},
		{"no-pre-vote", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := inmemConfig(t)
			conf.PreVoteDisabled = tc.preVoteDisabled
			c := MakeCluster(1, t, conf)
			defer c.Close()

			originalLeader := c.Leader()
			originalAddr := originalLeader.localAddr

			// Boot an un-bootstrapped node and join it through the standard
			// AddVoter membership change path.
			joined := MakeClusterNoBootstrap(1, t, conf)
			c.Merge(joined)
			c.FullyConnect()

			joinFuture := originalLeader.AddVoter(joined.rafts[0].localID, joined.rafts[0].localAddr, 0, 0)
			require.NoError(t, joinFuture.Error())

			// A committed write proves the two-voter quorum works and that the
			// follower has caught up; only then assert leadership visibility.
			write := c.Leader().Apply([]byte("after-join"), conf.CommitTimeout)
			require.NoError(t, write.Error())
			c.WaitForReplication(1)
			c.EnsureLeader(t, originalAddr)
			c.Followers()
			c.EnsureSamePeers(t)
			c.EnsureSame(t)

			// A second write keeps committing through the two-voter quorum,
			// showing the enlarged cluster behaves like a normal Raft group.
			secondWrite := c.Leader().Apply([]byte("after-join-2"), conf.CommitTimeout)
			require.NoError(t, secondWrite.Error())
			c.WaitForReplication(2)
			c.EnsureSame(t)
		})
	}
}

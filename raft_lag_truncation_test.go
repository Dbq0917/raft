// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowTransport wraps an InmemTransport to make one follower lag while it
// stays connected. Data-bearing AppendEntries RPCs are held behind a control
// gate so the follower stops absorbing new log entries, while heartbeats,
// elections and InstallSnapshot RPCs keep flowing on a fast path.
type slowTransport struct {
	*InmemTransport
	gate atomicGate
	cons chan RPC
	// electionDelay holds this node back at startup so it never becomes the
	// first leader of the cluster. It is cleared once a leader is established.
	electionDelay atomic.Bool
	stopC         chan struct{}
	stopOnce      sync.Once
}

// atomicGate is a one-shot latch controlling whether held-back AppendEntries
// RPCs may be delivered to the follower's raft main loop.
type atomicGate struct {
	once  sync.Once
	openC chan struct{}
}

func (g *atomicGate) init() {
	g.openC = make(chan struct{})
}

func (g *atomicGate) openGate() {
	g.once.Do(func() { close(g.openC) })
}

// stop terminates the pump and releases any goroutine parked on the gate so
// the cluster can shut down cleanly at the end of a test.
func (s *slowTransport) stop() {
	s.stopOnce.Do(func() {
		close(s.stopC)
		s.gate.openGate()
	})
}

// Consumer returns the channel handed to the raft main loop.
func (s *slowTransport) Consumer() <-chan RPC {
	return s.cons
}

// pump forwards RPCs from the underlying transport to the raft main loop.
// Data-bearing AppendEntries block on the control gate until it opens, while
// everything else (heartbeats, elections, InstallSnapshot) is forwarded
// immediately so the node stays a stable follower reachable for a snapshot.
func (s *slowTransport) pump() {
	for {
		select {
		case <-s.stopC:
			return
		case rpc, ok := <-s.InmemTransport.Consumer():
			if !ok {
				return
			}
			if req, ok := rpc.Command.(*AppendEntriesRequest); ok && len(req.Entries) > 0 {
				rpc := rpc
				go func() {
					select {
					case <-s.gate.openC:
					case <-s.stopC:
						return
					}
					select {
					case s.cons <- rpc:
					case <-s.stopC:
					}
				}()
				continue
			}
			s.cons <- rpc
		}
	}
}

func (s *slowTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	// Force the standard (non-pipeline) replication path, which is the path
	// that must recover from a compacted log by sending a snapshot.
	return nil, ErrPipelineReplicationNotSupported
}

// pipelineSlowTransport keeps the embedded in-memory transport's real
// pipeline support, exercising the pipeline replication path under lag and
// log compaction.
type pipelineSlowTransport struct {
	*slowTransport
}

func (p *pipelineSlowTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	return p.slowTransport.InmemTransport.AppendEntriesPipeline(id, target)
}

func (s *slowTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	if s.electionDelay.Load() {
		time.Sleep(200 * time.Millisecond)
	}
	return s.InmemTransport.RequestVote(id, target, args, resp)
}

func (s *slowTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	if s.electionDelay.Load() {
		time.Sleep(200 * time.Millisecond)
	}
	return s.InmemTransport.RequestPreVote(id, target, args, resp)
}

// newLagCluster builds a 3-server in-memory cluster in which the follower at
// lagIndex uses a controllable slowTransport. The in-memory RPC timeout is
// kept short so cluster shutdown can drain any in-flight RPCs promptly.
func newLagCluster(t *testing.T, conf *Config, lagIndex int, usePipeline bool) (*cluster, *slowTransport) {
	t.Helper()

	// The gate is opened well within this window, while a short timeout also
	// lets cluster shutdown drain any in-flight RPCs.
	const rpcTimeout = time.Second
	const peers = 3

	c := &cluster{
		observationCh:    make(chan Observation, 1024),
		conf:             conf,
		propagateTimeout: conf.HeartbeatTimeout*2 + conf.CommitTimeout,
		longstopTimeout:  10 * time.Second,
		logger:           newTestLoggerWithPrefix(t, "cluster"),
		failedCh:         make(chan struct{}),
		t:                t,
	}

	var configuration Configuration
	for i := 0; i < peers; i++ {
		dir, err := os.MkdirTemp("", "raft")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		store := NewInmemStore()
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		addr, trans := NewInmemTransportWithTimeout(NewInmemAddr(), rpcTimeout)
		c.trans = append(c.trans, trans)
		localID := ServerID(fmt.Sprintf("server-%s", addr))
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       localID,
			Address:  addr,
		})
	}
	c.FullyConnect()

	var slow *slowTransport
	for i := 0; i < peers; i++ {
		var trans LoopbackTransport = c.trans[i]
		if i == lagIndex {
			raw := c.trans[i].(*InmemTransport)
			slow = &slowTransport{InmemTransport: raw, cons: make(chan RPC, 4096), stopC: make(chan struct{})}
			slow.gate.init()
			slow.electionDelay.Store(true)
			go slow.pump()
			if usePipeline {
				pt := &pipelineSlowTransport{slowTransport: slow}
				trans = pt
				c.trans[i] = pt
			} else {
				trans = slow
				c.trans[i] = slow
			}
		}

		peerConf := *conf
		peerConf.LocalID = configuration.Servers[i].ID
		peerConf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[i].ID))

		if err := BootstrapCluster(&peerConf, c.stores[i], c.stores[i], c.snaps[i], trans, configuration); err != nil {
			t.Fatalf("BootstrapCluster failed: %v", err)
		}
		r, err := NewRaft(&peerConf, c.fsms[i], c.stores[i], c.stores[i], c.snaps[i], trans)
		if err != nil {
			t.Fatalf("NewRaft failed: %v", err)
		}
		r.RegisterObserver(NewObserver(c.observationCh, false, nil))
		c.rafts = append(c.rafts, r)
	}
	return c, slow
}

// runLaggingSnapshotTest drives the "connected follower falls behind, leader
// snapshots and compacts, follower self-heals from the snapshot" scenario.
// When usePipeline is true the leader is allowed to use pipelined
// AppendEntries; otherwise it is forced onto the standard replication path.
func runLaggingSnapshotTest(t *testing.T, usePipeline bool) {
	conf := inmemConfig(t)
	// Snapshot aggressively so a lagging follower quickly runs into a
	// compacted log range.
	conf.SnapshotInterval = 10 * time.Millisecond
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10

	c, slow := newLagCluster(t, conf, 2, usePipeline)
	defer func() {
		slow.stop()
		_ = slow.InmemTransport.Close()
		c.Close()
	}()

	leader := c.Leader()
	behind := c.rafts[2]
	if behind == nil {
		t.Fatalf("slow follower not found")
	}
	if behind == leader {
		t.Fatalf("slow node must not become leader")
	}
	// A stable leader exists, so the slow node no longer needs to be held back
	// from elections. It must remain a follower for the rest of the test.
	slow.electionDelay.Store(false)

	// With the gate closed the follower stops taking new log entries, while
	// heartbeats keep it a stable follower. The leader races ahead, snapshots
	// and compacts the entries this follower is asking for.
	var future Future
	for i := 0; i < 300; i++ {
		future = leader.Apply([]byte("test"), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for the leader to snapshot and compact its old logs.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snaps, err := leader.snapshots.List()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		first, _ := leader.logs.FirstIndex()
		if len(snaps) > 0 && first > 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	snaps, _ := leader.snapshots.List()
	if len(snaps) == 0 {
		t.Fatalf("leader did not take a snapshot")
	}
	firstIndex, _ := leader.logs.FirstIndex()
	if firstIndex <= 1 {
		t.Fatalf("leader did not compact old logs, first index %d", firstIndex)
	}

	// Confirm the follower is actually behind before healing.
	behindLast, _ := behind.logs.LastIndex()
	leaderLast, _ := leader.logs.LastIndex()
	if behindLast >= leaderLast {
		t.Fatalf("follower not behind: behind=%d leader=%d", behindLast, leaderLast)
	}
	t.Logf("follower last index %d, leader last index %d, snapshot index %d, first log index %d",
		behindLast, leaderLast, snaps[0].Index, firstIndex)

	// Open the gate: the follower must now self-heal via the snapshot and
	// catch up without being rebuilt manually.
	slow.gate.openGate()
	c.EnsureSame(t)
}

// TestRaft_LaggingFollower_RecoversFromSnapshot exercises the standard
// (non-pipeline) replication path.
func TestRaft_LaggingFollower_RecoversFromSnapshot(t *testing.T) {
	runLaggingSnapshotTest(t, false)
}

// TestRaft_LaggingFollower_RecoversFromSnapshot_Pipeline exercises the
// pipelined replication path, which is the default for healthy followers.
func TestRaft_LaggingFollower_RecoversFromSnapshot_Pipeline(t *testing.T) {
	runLaggingSnapshotTest(t, true)
}

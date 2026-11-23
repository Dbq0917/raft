// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAsyncLogCacheOrderedFlush(t *testing.T) {
	store := NewInmemStore()
	store.SetMonotonic(true)

	cache, err := NewLogCacheAsync(32, store)
	require.NoError(t, err)
	require.NoError(t, cache.StoreLogs(makeAsyncLogs(1, 5)))

	completions := make(chan LogWriteCompletion, 4)
	cache.EnableAsync(completions)

	unblock := store.BlockStore()
	require.NoError(t, cache.StoreLogsAsync(makeAsyncLogs(6, 8)))
	require.NoError(t, cache.StoreLogsAsync(makeAsyncLogs(9, 10)))

	assertAsyncLogContents(t, cache, 1, 10)
	assertAsyncLogContents(t, store, 1, 5)

	unblock()
	waitAsyncPersistent(t, completions, 10)
	assertAsyncLogContents(t, store, 1, 10)

	_, err = cache.LastIndex()
	require.NoError(t, err)
	require.NoError(t, cache.StoreLogsAsync(makeAsyncLogs(11, 15)))
	cache.DisableAsync()
	assertAsyncLogContents(t, store, 1, 15)
}

func TestAsyncLogCacheRejectsOutOfOrderWrites(t *testing.T) {
	store := NewInmemStore()
	cache, err := NewLogCacheAsync(32, store)
	require.NoError(t, err)
	require.NoError(t, cache.StoreLogs(makeAsyncLogs(1, 5)))

	completions := make(chan LogWriteCompletion, 1)
	cache.EnableAsync(completions)
	t.Cleanup(cache.DisableAsync)

	require.ErrorContains(t, cache.StoreLogsAsync(makeAsyncLogs(7, 8)), "non-contiguous")
	assertAsyncLogContents(t, cache, 1, 5)
}

func TestAsyncLogCacheRetriesTransientWriteError(t *testing.T) {
	store := NewInmemStore()
	cache, err := NewLogCacheAsync(32, store)
	require.NoError(t, err)
	require.NoError(t, cache.StoreLogs(makeAsyncLogs(1, 5)))

	completions := make(chan LogWriteCompletion, 4)
	cache.EnableAsync(completions)
	t.Cleanup(cache.DisableAsync)

	unblock := store.BlockStore()
	require.NoError(t, cache.StoreLogsAsync(makeAsyncLogs(6, 10)))
	store.FailNext()
	unblock()

	lwc := <-completions
	require.ErrorContains(t, lwc.Error, "IO error")
	require.Equal(t, uint64(5), lwc.PersistentIndex)

	waitAsyncPersistent(t, completions, 10)
	assertAsyncLogContents(t, store, 1, 10)
}

func TestRaftAsyncLogSlowLeaderDisk(t *testing.T) {
	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     3,
		Bootstrap: true,
		LogstoreWrapperFunc: func(logs LogStore) LogStore {
			cache, err := NewLogCacheAsync(256, logs)
			require.NoError(t, err)
			return cache
		},
	})
	t.Cleanup(c.Close)

	leader := c.Leader()
	cache, ok := leader.logs.(*LogCacheAsync)
	require.True(t, ok)

	require.NoError(t, leader.Apply([]byte("warmup"), 2*time.Second).Error())
	require.Eventually(t, func() bool {
		return cache.persistentIndex.Load() == cache.lastIndex.Load()
	}, 2*time.Second, 10*time.Millisecond)

	store := cache.store.(*InmemStore)
	unblock := store.BlockStore()
	defer unblock()

	const writes = 20
	futures := make([]ApplyFuture, 0, writes)
	for i := 0; i < writes; i++ {
		futures = append(futures, leader.Apply([]byte(fmt.Sprintf("write-%d", i)), 2*time.Second))
	}

	result := make(chan error, 1)
	go func() {
		for _, future := range futures {
			if err := future.Error(); err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("commit was blocked by the leader's local disk")
	}
}

func waitAsyncPersistent(t *testing.T, ch <-chan LogWriteCompletion, index uint64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case lwc := <-ch:
			require.NoError(t, lwc.Error)
			if lwc.PersistentIndex >= index {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for persistent index %d", index)
		}
	}
}

func assertAsyncLogContents(t *testing.T, store LogStore, min, max uint64) {
	t.Helper()

	first, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, min, first)
	last, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, max, last)

	for idx := min; idx <= max; idx++ {
		var log Log
		require.NoError(t, store.GetLog(idx, &log))
		require.Equal(t, idx, log.Index)
		require.Equal(t, fmt.Sprintf("op-%d", idx), string(log.Data))
	}
}

func makeAsyncLogs(min, max uint64) []*Log {
	logs := make([]*Log, 0, max-min+1)
	for idx := min; idx <= max; idx++ {
		logs = append(logs, &Log{
			Index: idx,
			Data:  []byte(fmt.Sprintf("op-%d", idx)),
		})
	}
	return logs
}

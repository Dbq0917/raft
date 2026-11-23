// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var _ AsyncLogStore = (*LogCacheAsync)(nil)

// LogCacheAsync is a LogCache that can accept ordered asynchronous writes.
//
// Appends are immediately visible through GetLog and LastIndex, while a single
// background goroutine flushes them to the wrapped LogStore in index order.
// This lets the leader replicate entries to followers before a slow local disk
// has finished persisting them. The leader only advances its local match index
// after the corresponding write completes.
type LogCacheAsync struct {
	store LogStore

	monotonic bool
	size      uint64
	sizeMask  uint64

	lastIndex       atomic.Uint64
	firstIndex      atomic.Uint64
	persistentIndex atomic.Uint64

	// writeMu serializes writes and deletes to the underlying store. Cache
	// state is protected separately so reads are not blocked by disk I/O.
	writeMu sync.Mutex

	state struct {
		sync.Mutex

		cache        []*Log
		completionCh chan<- LogWriteCompletion
		updates      chan LogWriteCompletion
		stopBridge   chan struct{}
		triggerCh    chan syncRequest
		asyncErr     error
	}
}

type syncRequest struct {
	startTime time.Time
	stop      bool
	doneCh    chan struct{}
}

// NewLogCacheAsync creates an asynchronous cache in front of store. capacity is
// rounded up to a power of two and must be large enough to hold all writes that
// can be in flight while the underlying store is slow.
func NewLogCacheAsync(capacity int, store LogStore) (*LogCacheAsync, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}

	first, err := store.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := store.LastIndex()
	if err != nil {
		return nil, err
	}

	size := uint64(1)
	for size < uint64(capacity) {
		size <<= 1
	}

	c := &LogCacheAsync{
		store:    store,
		size:     size,
		sizeMask: size - 1,
	}
	c.firstIndex.Store(first)
	c.lastIndex.Store(last)
	c.persistentIndex.Store(last)
	c.state.cache = make([]*Log, size)
	c.state.triggerCh = make(chan syncRequest, 1)

	if ms, ok := store.(MonotonicLogStore); ok {
		c.monotonic = ms.IsMonotonic()
	}
	return c, nil
}

// IsMonotonic implements MonotonicLogStore.
func (c *LogCacheAsync) IsMonotonic() bool {
	return c.monotonic
}

// FirstIndex returns the first readable log index.
func (c *LogCacheAsync) FirstIndex() (uint64, error) {
	if idx := c.firstIndex.Load(); idx != 0 {
		return idx, nil
	}
	return c.store.FirstIndex()
}

// LastIndex returns the last readable log index, including writes that have
// not yet been persisted.
func (c *LogCacheAsync) LastIndex() (uint64, error) {
	return c.lastIndex.Load(), nil
}

// GetLog returns a log entry from the cache when possible and falls back to the
// underlying store otherwise.
func (c *LogCacheAsync) GetLog(index uint64, log *Log) error {
	c.state.Lock()
	cached := c.state.cache[index&c.sizeMask]
	c.state.Unlock()

	if cached != nil && cached.Index == index {
		*log = *cached
		return nil
	}
	return c.store.GetLog(index, log)
}

// StoreLog implements LogStore.
func (c *LogCacheAsync) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

// StoreLogs writes logs synchronously. It is only valid while the cache is not
// in asynchronous mode.
func (c *LogCacheAsync) StoreLogs(logs []*Log) error {
	if len(logs) == 0 {
		return nil
	}

	c.state.Lock()
	isAsync := c.state.completionCh != nil
	c.state.Unlock()
	if isAsync {
		return errors.New("call to sync StoreLog(s) when in async mode")
	}

	c.writeMu.Lock()
	err := c.store.StoreLogs(logs)
	c.writeMu.Unlock()
	if err != nil {
		return err
	}

	c.state.Lock()
	for _, l := range logs {
		c.state.cache[l.Index&c.sizeMask] = l
	}
	first := logs[0].Index
	if c.firstIndex.Load() == 0 {
		c.firstIndex.Store(first)
	}
	last := logs[len(logs)-1].Index
	c.lastIndex.Store(last)
	c.persistentIndex.Store(last)
	c.state.Unlock()
	return nil
}

// StoreLogsAsync makes logs readable immediately and queues them for an ordered
// background flush.
func (c *LogCacheAsync) StoreLogsAsync(logs []*Log) error {
	if len(logs) == 0 {
		return nil
	}

	for {
		c.state.Lock()
		if c.state.completionCh == nil {
			c.state.Unlock()
			return errors.New("call to StoreLogsAsync when in sync mode")
		}
		if c.state.asyncErr != nil {
			err := c.state.asyncErr
			c.state.Unlock()
			return err
		}

		lastIdx := c.lastIndex.Load()
		persistedIdx := c.persistentIndex.Load()
		if !hasSpaceFor(len(logs), lastIdx, persistedIdx, c.size) {
			c.state.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}

		if logs[0].Index != lastIdx+1 {
			c.state.Unlock()
			return fmt.Errorf("non-contiguous async log write: first index %d, last index %d",
				logs[0].Index, lastIdx)
		}
		for i := 1; i < len(logs); i++ {
			if logs[i].Index != logs[i-1].Index+1 {
				c.state.Unlock()
				return fmt.Errorf("non-contiguous async log write at index %d", logs[i].Index)
			}
		}

		for _, log := range logs {
			c.state.cache[log.Index&c.sizeMask] = log
		}
		if c.firstIndex.Load() == 0 {
			c.firstIndex.Store(logs[0].Index)
		}
		c.lastIndex.Store(logs[len(logs)-1].Index)

		select {
		case c.state.triggerCh <- syncRequest{startTime: time.Now()}:
		default:
		}
		c.state.Unlock()
		return nil
	}
}

func hasSpaceFor(n int, lastIdx, persistIdx, size uint64) bool {
	if uint64(n) > size {
		panic("trying to write more logs than will ever fit in the cache")
	}
	newLastIdx := lastIdx + uint64(n)
	return newLastIdx-persistIdx <= size
}

// EnableAsync implements AsyncLogStore.
func (c *LogCacheAsync) EnableAsync(completionCh chan<- LogWriteCompletion) {
	if completionCh == nil {
		panic("nil async completion channel")
	}

	c.state.Lock()
	defer c.state.Unlock()
	if c.state.completionCh != nil {
		panic("already in async mode")
	}
	if c.persistentIndex.Load() != c.lastIndex.Load() {
		panic("cannot enable async mode with unpersisted logs")
	}

	c.state.completionCh = completionCh
	c.state.updates = make(chan LogWriteCompletion, 8)
	c.state.stopBridge = make(chan struct{})
	c.state.asyncErr = nil
	go c.runFlusher()
	go c.runCompletionBridge()
}

// DisableAsync implements AsyncLogStore.
func (c *LogCacheAsync) DisableAsync() {
	c.state.Lock()
	if c.state.completionCh == nil {
		c.state.Unlock()
		panic("already in sync mode")
	}
	c.state.Unlock()

	doneCh := make(chan struct{})
	c.state.triggerCh <- syncRequest{
		startTime: time.Now(),
		stop:      true,
		doneCh:    doneCh,
	}
	<-doneCh
}

func (c *LogCacheAsync) runFlusher() {
	for req := range c.state.triggerCh {
		c.flush(req)
		if req.stop {
			return
		}
	}
}

func (c *LogCacheAsync) runCompletionBridge() {
	c.state.Lock()
	updates := c.state.updates
	out := c.state.completionCh
	stop := c.state.stopBridge
	c.state.Unlock()

	var pending []LogWriteCompletion
	for {
		var next chan<- LogWriteCompletion
		var nextCompletion LogWriteCompletion
		if len(pending) > 0 {
			next = out
			nextCompletion = pending[0]
		}
		select {
		case lwc := <-updates:
			pending = append(pending, lwc)
		case next <- nextCompletion:
			pending = pending[1:]
		case <-stop:
			return
		}
	}
}

func (c *LogCacheAsync) flush(req syncRequest) {
	for {
		c.state.Lock()
		if req.stop && c.state.completionCh == nil {
			c.state.Unlock()
			close(req.doneCh)
			return
		}

		persistedIdx := c.persistentIndex.Load()
		lastIdx := c.lastIndex.Load()
		if persistedIdx >= lastIdx {
			if req.stop {
				c.stopAsyncLocked()
			}
			if req.doneCh != nil {
				close(req.doneCh)
			}
			c.state.Unlock()
			return
		}

		batch := make([]*Log, 0, lastIdx-persistedIdx)
		for idx := persistedIdx + 1; idx <= lastIdx; idx++ {
			batch = append(batch, c.state.cache[idx&c.sizeMask])
		}
		c.state.Unlock()

		lwc := c.doFlush(batch, req.startTime)

		c.state.Lock()
		if lwc.Error != nil {
			c.state.asyncErr = lwc.Error
		} else {
			c.persistentIndex.Store(lwc.PersistentIndex)
			c.state.asyncErr = nil
		}
		if !req.stop {
			c.deliverCompletionLocked(lwc)
		}

		if lwc.Error != nil {
			if req.stop {
				c.stopAsyncLocked()
			}
			if req.doneCh != nil {
				close(req.doneCh)
			}
			c.state.Unlock()

			if !req.stop {
				// Retry transient write failures while preserving the same
				// ordered prefix. The leader is notified of the error above and
				// may step down, in which case DisableAsync will stop the retry
				// loop.
				select {
				case c.state.triggerCh <- syncRequest{startTime: time.Now()}:
				default:
				}
			}
			return
		}

		if req.stop {
			if c.persistentIndex.Load() >= c.lastIndex.Load() {
				c.stopAsyncLocked()
				if req.doneCh != nil {
					close(req.doneCh)
				}
				c.state.Unlock()
				return
			}
			c.state.Unlock()
			continue
		}

		if c.persistentIndex.Load() >= c.lastIndex.Load() {
			if req.doneCh != nil {
				close(req.doneCh)
			}
			c.state.Unlock()
			return
		}
		c.state.Unlock()
	}
}

func (c *LogCacheAsync) doFlush(logs []*Log, start time.Time) LogWriteCompletion {
	lwc := LogWriteCompletion{
		PersistentIndex: logs[0].Index - 1,
		Duration:        time.Since(start),
	}

	c.writeMu.Lock()
	lwc.Error = c.store.StoreLogs(logs)
	c.writeMu.Unlock()
	if lwc.Error == nil {
		lwc.PersistentIndex = logs[len(logs)-1].Index
	}
	return lwc
}

func (c *LogCacheAsync) deliverCompletionLocked(lwc LogWriteCompletion) {
	if c.state.completionCh == nil {
		return
	}
	// The bridge always continues draining updates while it is blocked on the
	// leader-facing channel, so this cannot stall disk flushing if the leader
	// loop is busy.
	c.state.updates <- lwc
}

func (c *LogCacheAsync) stopAsyncLocked() {
	c.state.completionCh = nil
	if c.state.stopBridge != nil {
		close(c.state.stopBridge)
		c.state.stopBridge = nil
	}
}

// DeleteRange deletes logs from the cache and underlying store. Pending async
// writes are flushed first so a delete can never race with an older write and
// resurrect logs out of order.
func (c *LogCacheAsync) DeleteRange(min, max uint64) error {
	c.state.Lock()
	isAsync := c.state.completionCh != nil
	c.state.Unlock()

	if isAsync {
		for {
			c.state.Lock()
			if c.state.asyncErr != nil {
				err := c.state.asyncErr
				c.state.Unlock()
				return err
			}
			if c.persistentIndex.Load() >= c.lastIndex.Load() {
				break
			}
			c.state.Unlock()
			time.Sleep(time.Millisecond)
		}
	} else {
		c.state.Lock()
	}
	defer c.state.Unlock()

	c.writeMu.Lock()
	err := c.store.DeleteRange(min, max)
	c.writeMu.Unlock()
	if err != nil {
		return err
	}

	for idx := min; idx <= max; idx++ {
		cached := c.state.cache[idx&c.sizeMask]
		if cached != nil && cached.Index == idx {
			c.state.cache[idx&c.sizeMask] = nil
		}
	}

	first := c.firstIndex.Load()
	if first != 0 && min <= first {
		next := max + 1
		if next > c.lastIndex.Load() {
			next = 0
		}
		c.firstIndex.Store(next)
	}
	return nil
}

// StageCommitIndex forwards the optional CommitTrackingLogStore interface.
func (c *LogCacheAsync) StageCommitIndex(index uint64) error {
	store, ok := c.store.(CommitTrackingLogStore)
	if !ok {
		return nil
	}
	return store.StageCommitIndex(index)
}

// GetCommitIndex forwards the optional CommitTrackingLogStore interface.
func (c *LogCacheAsync) GetCommitIndex() (uint64, error) {
	store, ok := c.store.(CommitTrackingLogStore)
	if !ok {
		return 0, nil
	}
	return store.GetCommitIndex()
}

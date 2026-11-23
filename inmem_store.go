// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var _ CommitTrackingLogStore = &InmemCommitTrackingStore{}

// InmemStore implements the LogStore and StableStore interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
type InmemStore struct {
	storeFail atomic.Bool
	storeSem  chan struct{}

	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*Log
	kv        map[string][]byte
	kvInt     map[string]uint64
	monotonic bool
}

// NewInmemStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewInmemStore() *InmemStore {
	i := &InmemStore{
		storeSem: make(chan struct{}, 1),
		logs:     make(map[uint64]*Log),
		kv:       make(map[string][]byte),
		kvInt:    make(map[string]uint64),
	}
	return i
}

// BlockStore causes subsequent StoreLog and StoreLogs calls to block until the
// returned function is called.
func (i *InmemStore) BlockStore() func() {
	i.storeSem <- struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() { <-i.storeSem })
	}
}

// FailNext makes the next StoreLog or StoreLogs call return an error without
// modifying the store.
func (i *InmemStore) FailNext() {
	i.storeFail.Store(true)
}

// FirstIndex implements the LogStore interface.
func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

// LastIndex implements the LogStore interface.
func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

// GetLog implements the LogStore interface.
func (i *InmemStore) GetLog(index uint64, log *Log) error {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// StoreLog implements the LogStore interface.
func (i *InmemStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
func (i *InmemStore) StoreLogs(logs []*Log) error {
	i.storeSem <- struct{}{}
	defer func() { <-i.storeSem }()

	if i.storeFail.Swap(false) {
		return errors.New("IO error")
	}

	i.l.Lock()
	defer i.l.Unlock()
	for _, l := range logs {
		if i.monotonic && l.Index != i.highIndex+1 {
			return fmt.Errorf("non-monotonic write, log index: %d, last index %d, batch range: [%d, %d]",
				l.Index, i.highIndex, logs[0].Index, logs[len(logs)-1].Index)
		}
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}
	if min <= i.lowIndex {
		i.lowIndex = max + 1
	}
	if max >= i.highIndex {
		i.highIndex = min - 1
	}
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}

// Set implements the StableStore interface.
func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

// Get implements the StableStore interface.
func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	val := i.kv[string(key)]
	if val == nil {
		return nil, errors.New("not found")
	}
	return val, nil
}

// SetUint64 implements the StableStore interface.
func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

// GetUint64 implements the StableStore interface.
func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}

// SetMonotonic controls whether the store rejects non-contiguous writes.
func (i *InmemStore) SetMonotonic(v bool) {
	i.l.Lock()
	defer i.l.Unlock()
	i.monotonic = v
}

type InmemCommitTrackingStore struct {
	InmemStore
	commitIndex atomic.Uint64
}

// NewInmemCommitTrackingStore returns a new in-memory backend that tracks the commit index. Do not ever
// use for production. Only for testing.
func NewInmemCommitTrackingStore() *InmemCommitTrackingStore {
	i := &InmemCommitTrackingStore{
		InmemStore: *NewInmemStore(),
	}
	return i
}

func (i *InmemCommitTrackingStore) StageCommitIndex(index uint64) error {
	i.commitIndex.Store(index)
	return nil
}

func (i *InmemCommitTrackingStore) GetCommitIndex() (uint64, error) {
	return i.commitIndex.Load(), nil
}

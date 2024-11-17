package mvcc

import (
	"errors"
	"sync"
)

// SerializableLock represents a lock for a query range.
type SerializableLock struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

// NewSerializableLock creates a new lock instance.
func NewSerializableLock() *SerializableLock {
	return &SerializableLock{locks: make(map[string]struct{})}
}

// LockRange locks a range for a transaction.
func (s *SerializableLock) LockRange(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.locks[key]; exists {
		return errors.New("range already locked")
	}
	s.locks[key] = struct{}{}
	return nil
}

// UnlockRange unlocks a range.
func (s *SerializableLock) UnlockRange(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.locks, key)
}

// GetVersions returns the versions of the record for testing purposes.
func (r *Record) GetVersions() []*Version {
	return r.Versions
}

func (r *Record) GetAllVersions() []*Version {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := []*Version{}
	for _, version := range r.Versions {
		result = append(result, version)	
	}
	return result
}

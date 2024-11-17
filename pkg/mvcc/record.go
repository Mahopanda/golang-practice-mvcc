package mvcc

import (
	"sync"
)

type Record struct {
	mu           sync.RWMutex
	versionChain *VersionChain
}

func NewRecord() *Record {
	return &Record{
		versionChain: NewVersionChain(),
	}
}

func (r *Record) InsertVersion(value string, ts int, txID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	version := &Version{
		Value:     value,
		Timestamp: ts,
		TxID:      txID,
		Committed: false,
	}

	r.versionChain.AddVersion(version)
	return nil
}

func (r *Record) GetVersion(ts int, isolationLevel IsolationLevel) (*Version, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.versionChain.GetVersion(ts, isolationLevel)
}

func (r *Record) CommitVersion(txID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.versionChain.GetVersions()
	for _, v := range versions {
		if v.TxID == txID && !v.Committed {
			v.Committed = true
			return nil
		}
	}
	return ErrVersionNotFound
}

func (r *Record) CleanupVersions(oldestActiveTS int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.versionChain.CleanupVersions(oldestActiveTS)
}

// GetVersions returns all versions (for testing)
func (r *Record) GetVersions() []*Version {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.versionChain.GetVersions()
}
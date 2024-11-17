package mvcc

import (
    "errors"
    "sync"
)

type LockType int

const (
    ReadLock LockType = iota
    WriteLock
)

// LockManager 管理鎖
type LockManager struct {
    locks map[string]map[int]LockType  // key -> txID -> lockType
    mu    sync.RWMutex
}

func NewLockManager() *LockManager {
    return &LockManager{
        locks: make(map[string]map[int]LockType),
    }
}

func (lm *LockManager) AcquireLock(txID int, key string, lockType LockType) error {
    lm.mu.Lock()
    defer lm.mu.Unlock()

    if _, exists := lm.locks[key]; !exists {
        lm.locks[key] = make(map[int]LockType)
    }

    // 檢查鎖衝突
    for tid, existingLock := range lm.locks[key] {
        if tid != txID {
            if existingLock == WriteLock || lockType == WriteLock {
                return errors.New("lock conflict")
            }
        }
    }

    lm.locks[key][txID] = lockType
    return nil
}

func (lm *LockManager) ReleaseLock(txID int, key string) {
    lm.mu.Lock()
    defer lm.mu.Unlock()

    if keyLocks, exists := lm.locks[key]; exists {
        delete(keyLocks, txID)
        if len(keyLocks) == 0 {
            delete(lm.locks, key)
        }
    }
} 
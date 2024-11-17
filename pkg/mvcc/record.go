package mvcc

import (
	"errors"
	"sync"
)

// Version 定義數據版本
type Version struct {
	Value     string
	Timestamp int
	EndTS     int
	Committed bool
}

// Record 定義數據記錄
type Record struct {
	mu       sync.RWMutex
	Versions []*Version // 改為公開，方便測試
}

func NewRecord() *Record {
	return &Record{
		Versions: make([]*Version, 0),
	}
}

// InsertVersion 插入新版本
func (r *Record) InsertVersion(value string, ts int, committed bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 檢查時間戳衝突
	for _, v := range r.Versions {
		if v.Timestamp == ts {
			return errors.New("version timestamp conflict")
		}
	}

	// 為上一個版本設置結束時間戳
	if len(r.Versions) > 0 {
		r.Versions[len(r.Versions)-1].EndTS = ts
	}

	r.Versions = append(r.Versions, &Version{
		Value:     value,
		Timestamp: ts,
		EndTS:     0,
		Committed: committed,
	})
	return nil
}

// CommitVersion 提交指定時間戳的版本
func (r *Record) CommitVersion(ts int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, v := range r.Versions {
		if v.Timestamp == ts {
			v.Committed = true
			return nil
		}
	}
	return errors.New("version not found")
}

// GetVersion 獲取指定時間戳的版本
func (r *Record) GetVersion(ts int) (*Version, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var latest *Version
	for _, v := range r.Versions {
		if v.Timestamp <= ts && v.Committed {
			if latest == nil || v.Timestamp > latest.Timestamp {
				latest = v
			}
		}
	}
	if latest == nil {
		return nil, errors.New("no valid version found")
	}
	return latest, nil
}

// CleanupVersions 清理舊版本
func (r *Record) CleanupVersions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Versions) <= 1 {
		return
	}

	// 只保留最新的已提交版本
	var latest *Version
	for _, v := range r.Versions {
		if v.Committed {
			if latest == nil || v.Timestamp > latest.Timestamp {
				latest = v
			}
		}
	}

	if latest != nil {
		r.Versions = []*Version{latest}
	}
}


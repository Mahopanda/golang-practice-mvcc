package mvcc

import (
	"sync"
)

// Version 定義數據版本
type Version struct {
	Value     string
	Timestamp int  // 創建時間戳
	EndTS     int  // 結束時間戳，0表示當前有效
	Committed bool // 是否已提交
	TxID      int  // 創建該版本的事務ID
}

// VersionChain 管理版本鏈
type VersionChain struct {
	versions []*Version
	mu       sync.RWMutex
}

func NewVersionChain() *VersionChain {
	return &VersionChain{
		versions: make([]*Version, 0),
	}
}

// AddVersion 添加新版本
func (vc *VersionChain) AddVersion(v *Version) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	if len(vc.versions) > 0 {
		// 設置前一個版本的結束時間戳
		vc.versions[len(vc.versions)-1].EndTS = v.Timestamp
	}
	vc.versions = append(vc.versions, v)
}

// GetVersion 根據時間戳獲取對應版本
func (vc *VersionChain) GetVersion(ts int, isolationLevel IsolationLevel) (*Version, error) {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	if len(vc.versions) == 0 {
		return nil, ErrVersionNotFound
	}

	switch isolationLevel {
	case ReadUncommitted:
		return vc.versions[len(vc.versions)-1], nil
	case ReadCommitted:
		// 找最新的已提交版本
		for i := len(vc.versions) - 1; i >= 0; i-- {
			if vc.versions[i].Committed {
				return vc.versions[i], nil
			}
		}
	case RepeatableRead, Serializable:
		// 找小於等於讀取時間戳的最新已提交版本
		for i := len(vc.versions) - 1; i >= 0; i-- {
			v := vc.versions[i]
			if v.Timestamp <= ts && v.Committed {
				return v, nil
			}
		}
	}
	return nil, ErrVersionNotFound
}

// CleanupVersions 清理過期版本
func (vc *VersionChain) CleanupVersions(oldestActiveTS int) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	if len(vc.versions) <= 1 {
		return
	}

	// 找到最後一個需要保留的版本索引
	keepIndex := 0
	for i := len(vc.versions) - 1; i >= 0; i-- {
		v := vc.versions[i]
		if v.Timestamp < oldestActiveTS && v.Committed {
			keepIndex = i
			break
		}
	}

	// 至少保留一個已提交的版本
	if keepIndex > 0 {
		// 確保保留的是已提交的版本
		hasCommitted := false
		for i := keepIndex; i < len(vc.versions); i++ {
			if vc.versions[i].Committed {
				hasCommitted = true
				break
			}
		}

		if hasCommitted {
			vc.versions = vc.versions[keepIndex:]
		}
	}
}

// GetVersions 獲取所有版本（用於測試）
func (vc *VersionChain) GetVersions() []*Version {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.versions
}

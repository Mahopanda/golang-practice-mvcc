// 資料庫邏輯
package mvcc

import (
	"errors"
	"sync"
)

// Database 定義MVCC數據庫
type Database struct {
	data      map[string]*Record
	currentTS int           // 全局時間戳
	mu        sync.RWMutex
}

// NewDatabase 創建新數據庫實例
func NewDatabase() *Database {
	return &Database{
		data: make(map[string]*Record),
	}
}

// Begin 開始新事務
func (db *Database) Begin() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	db.currentTS++
	return &Transaction{
		ID:      db.currentTS,
		ReadTS:  db.currentTS,
		WriteTS: db.currentTS,
	}
}

// Write 寫入數據
func (db *Database) Write(tx *Transaction, key, value string) error {
	if tx == nil {
		return errors.New("invalid transaction")
	}

	db.mu.Lock()
	record, exists := db.data[key]
	if !exists {
		record = NewRecord()
		db.data[key] = record
	}
	db.mu.Unlock()

	return record.InsertVersion(value, tx.WriteTS, false)
}

// Commit 提交事務
func (db *Database) Commit(tx *Transaction) error {
	if tx == nil {
		return errors.New("invalid transaction")
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, record := range db.data {
		if err := record.CommitVersion(tx.WriteTS); err != nil {
			return err
		}
	}
	return nil
}

// Read 讀取數據
func (db *Database) Read(tx *Transaction, key string) (string, error) {
	if tx == nil {
		return "", errors.New("invalid transaction")
	}

	db.mu.RLock()
	record, exists := db.data[key]
	if !exists {
		db.mu.RUnlock()
		return "", errors.New("key not found")
	}
	db.mu.RUnlock()

	version, err := record.GetVersion(tx.ReadTS)
	if err != nil {
		return "", err
	}
	return version.Value, nil
}

// CleanupOldVersions 執行垃圾回收
func (db *Database) CleanupOldVersions() {
	db.mu.RLock()
	records := make([]*Record, 0, len(db.data))
	for _, record := range db.data {
		records = append(records, record)
	}
	db.mu.RUnlock()

	for _, record := range records {
		record.CleanupVersions()
	}
}

// GetData returns the internal data map for testing purposes
func (db *Database) GetData() map[string]*Record {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data
}

// AdvanceTime advances the current timestamp
func (db *Database) AdvanceTime(amount int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.currentTS += amount
}

// Rollback 回滾事務
func (db *Database) Rollback(tx *Transaction) error {
	if tx == nil {
		return errors.New("invalid transaction")
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, record := range db.data {
		record.mu.Lock()
		newVersions := make([]*Version, 0)
		for _, v := range record.Versions {
			if v.Timestamp != tx.WriteTS {
				newVersions = append(newVersions, v)
			}
		}
		record.Versions = newVersions
		record.mu.Unlock()
	}
	return nil
}


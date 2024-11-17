// 資料庫邏輯
package mvcc

import (
	"errors"
	"sync"
)

// Database 定義MVCC數據庫
type Database struct {
	data        map[string]*Record
	currentTS   int
	mu          sync.RWMutex
	txManager   *TransactionManager
	lockManager *LockManager
}

// 新增事務管理器
type TransactionManager struct {
	activeTransactions map[int]*Transaction
	mu                 sync.RWMutex
}

// NewDatabase 創建新數據庫實例
func NewDatabase() *Database {
	return &Database{
		data:        make(map[string]*Record),
		txManager:   NewTransactionManager(),
		lockManager: NewLockManager(),
	}
}

// Begin 開始新事務
func (db *Database) Begin(level IsolationLevel) *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.currentTS++
	tx := NewTransaction(db.currentTS, level)
	db.txManager.AddTransaction(tx)
	return tx
}

// Write 寫入數據
func (db *Database) Write(tx *Transaction, key, value string) error {
	if err := db.validateTransaction(tx); err != nil {
		return err
	}

	// 檢查寫鎖
	if err := db.acquireLock(tx, key, WriteLock); err != nil {
		return err
	}

	db.mu.Lock()
	record, exists := db.data[key]
	if !exists {
		record = NewRecord()
		db.data[key] = record
	}
	db.mu.Unlock()

	// 記錄寫集
	tx.WriteSet[key] = value

	return record.InsertVersion(value, tx.WriteTS, tx.ID)
}

// Commit 提交事務
func (db *Database) Commit(tx *Transaction) error {
	// First phase: Prepare
	if err := db.prepare(tx); err != nil {
		return db.Rollback(tx)
	}

	// Second phase: Commit
	db.mu.Lock()
	defer db.mu.Unlock()

	// Commit changes for keys in WriteSet
	for key := range tx.WriteSet {
		record := db.data[key]
		if err := record.CommitVersion(tx.ID); err != nil {
			return db.Rollback(tx)
		}
		// Release write lock for this key
		db.lockManager.ReleaseLock(tx.ID, key)
	}

	// Release read locks for keys in ReadSet
	for key := range tx.ReadSet {
		db.lockManager.ReleaseLock(tx.ID, key)
	}

	tx.Status = Committed
	db.txManager.RemoveTransaction(tx.ID)
	return nil
}

func (db *Database) prepare(tx *Transaction) error {
	// 驗證讀集
	for key, ts := range tx.ReadSet {
		if !db.validateReadSet(tx, key, ts) {
			return ErrSerializationFailure
		}
	}
	return nil
}

// Read 讀取數據
func (db *Database) Read(tx *Transaction, key string) (string, error) {
	if err := db.validateTransaction(tx); err != nil {
		return "", err
	}

	// 根據隔離級別獲取適當的讀鎖
	if err := db.acquireLock(tx, key, ReadLock); err != nil {
		return "", err
	}

	db.mu.RLock()
	record, exists := db.data[key]
	if !exists {
		db.mu.RUnlock()
		return "", ErrKeyNotFound
	}
	db.mu.RUnlock()

	// 根據隔離級別讀取適當的版本
	version, err := record.GetVersion(tx.ReadTS, tx.IsolationLevel)
	if err != nil {
		return "", err
	}

	// 記錄讀集
	tx.ReadSet[key] = version.Timestamp

	return version.Value, nil
}

// CleanupOldVersions 執行垃圾回收
func (db *Database) CleanupOldVersions() {
	db.mu.RLock()
	oldestActiveTS := db.getOldestActiveTS()
	records := make([]*Record, 0, len(db.data))
	for _, record := range db.data {
		records = append(records, record)
	}
	db.mu.RUnlock()

	for _, record := range records {
		record.CleanupVersions(oldestActiveTS)
	}
}

// getOldestActiveTS 獲取最舊的活躍事務時間戳
func (db *Database) getOldestActiveTS() int {
	db.txManager.mu.RLock()
	defer db.txManager.mu.RUnlock()

	oldestTS := db.currentTS
	for _, tx := range db.txManager.activeTransactions {
		if tx.ReadTS < oldestTS {
			oldestTS = tx.ReadTS
		}
	}
	return oldestTS
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

	// Undo changes for the keys in WriteSet
	for key := range tx.WriteSet {
		record := db.data[key]
		record.mu.Lock()
		newVersions := make([]*Version, 0)
		for _, v := range record.versionChain.versions {
			if v.Timestamp != tx.WriteTS {
				newVersions = append(newVersions, v)
			}
		}
		record.versionChain.versions = newVersions
		record.mu.Unlock()
		// Release write lock for this key
		db.lockManager.ReleaseLock(tx.ID, key)
	}

	// Release read locks for the keys in ReadSet
	for key := range tx.ReadSet {
		db.lockManager.ReleaseLock(tx.ID, key)
	}

	return nil
}

// 增加2PL支持
func (db *Database) acquireLock(tx *Transaction, key string, lockType LockType) error {
	if tx.IsolationLevel == Serializable {
		return nil
	}
	return db.lockManager.AcquireLock(tx.ID, key, lockType)
}

// 添加驗證事務的方法
func (db *Database) validateTransaction(tx *Transaction) error {
	if tx == nil {
		return ErrInvalidTransaction
	}
	_, err := db.txManager.GetTransaction(tx.ID)
	return err
}

// 添加驗證讀集的方法
func (db *Database) validateReadSet(tx *Transaction, key string, ts int) bool {
	record, exists := db.data[key]
	if !exists {
		return true
	}

	versions := record.GetVersions()
	for _, v := range versions {
		if v.Timestamp > ts && v.Committed {
			return false
		}
	}
	return true
}

// 添加 ReadWithIsolation 方法
func (db *Database) ReadWithIsolation(tx *Transaction, key string, level IsolationLevel) (string, error) {
	if err := db.validateTransaction(tx); err != nil {
		return "", err
	}

	db.mu.RLock()
	record, exists := db.data[key]
	if !exists {
		db.mu.RUnlock()
		return "", ErrKeyNotFound
	}
	db.mu.RUnlock()

	version, err := record.GetVersion(tx.ReadTS, level)
	if err != nil {
		return "", err
	}

	tx.ReadSet[key] = version.Timestamp
	return version.Value, nil
}

// CountRange 計算範圍內的數據量
func (db *Database) CountRange(start, end string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	count := 0
	for key := range db.data {
		if key >= start && key <= end {
			count++
		}
	}
	return count
}

// 隔離級別邏輯
package mvcc

import "errors"

// IsolationLevel defines the types of transaction isolation levels.
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// ReadWithIsolation handles reading based on isolation levels.
func (db *Database) ReadWithIsolation(tx *Transaction, key string, level IsolationLevel) (string, error) {
	db.mu.RLock()
	record, exists := db.data[key]
	if !exists {
		db.mu.RUnlock()
		return "", errors.New("key not found")
	}
	db.mu.RUnlock()

	switch level {
	case ReadUncommitted:
		// 讀取最新版本，不管是否提交
		record.mu.RLock()
		defer record.mu.RUnlock()
		if len(record.Versions) == 0 {
			return "", errors.New("no versions available")
		}
		return record.Versions[len(record.Versions)-1].Value, nil

	case ReadCommitted:
		// 只讀取已提交的版本
		version, err := record.GetVersion(db.currentTS)
		if err != nil {
			return "", err
		}
		return version.Value, nil

	case RepeatableRead:
		// 使用事務開始時的時間戳讀取
		version, err := record.GetVersion(tx.ReadTS)
		if err != nil {
			return "", err
		}
		return version.Value, nil

	default:
		return "", errors.New("unsupported isolation level")
	}
}

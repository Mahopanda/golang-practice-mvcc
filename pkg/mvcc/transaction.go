// 事務邏輯
package mvcc

// Transaction represents a single transaction.
type Transaction struct {
	ID      int // Transaction ID
	ReadTS  int // Read timestamp
	WriteTS int // Write timestamp
}

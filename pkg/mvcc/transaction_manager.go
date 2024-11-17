package mvcc

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		activeTransactions: make(map[int]*Transaction),
	}
}

func (tm *TransactionManager) AddTransaction(tx *Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.activeTransactions[tx.ID] = tx
}

func (tm *TransactionManager) RemoveTransaction(txID int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.activeTransactions, txID)
}

func (tm *TransactionManager) GetTransaction(txID int) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tx, exists := tm.activeTransactions[txID]
	if !exists {
		return nil, ErrInvalidTransaction
	}
	return tx, nil
}

package mvcc

type Transaction struct {
	ID             int
	ReadTS         int
	WriteTS        int
	IsolationLevel IsolationLevel
	ReadSet        map[string]int    // 記錄讀取的key和版本
	WriteSet       map[string]string // 記錄寫入的key和值
	Status         TransactionStatus
}

type TransactionStatus int

const (
	Active TransactionStatus = iota
	Committed
	Aborted
)

func NewTransaction(id int, level IsolationLevel) *Transaction {
	return &Transaction{
		ID:             id,
		ReadTS:         id,
		WriteTS:        id,
		IsolationLevel: level,
		ReadSet:        make(map[string]int),
		WriteSet:       make(map[string]string),
		Status:         Active,
	}
}

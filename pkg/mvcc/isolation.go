package mvcc

// IsolationLevel 定義事務隔離級別
type IsolationLevel int

const (
    ReadUncommitted IsolationLevel = iota
    ReadCommitted
    RepeatableRead
    Serializable
)

// 定義常見的並發異常
type ConcurrencyError string

const (
    DirtyRead      ConcurrencyError = "dirty read"
    NonRepeatableRead ConcurrencyError = "non-repeatable read"
    PhantomRead    ConcurrencyError = "phantom read"
)
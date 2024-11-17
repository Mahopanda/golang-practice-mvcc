package mvcc

import "errors"

var (
    ErrVersionNotFound      = errors.New("version not found")
    ErrKeyNotFound         = errors.New("key not found")
    ErrSerializationFailure = errors.New("serialization failure")
    ErrInvalidTransaction  = errors.New("invalid transaction")
) 
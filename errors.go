package tinykv

import "errors"

// 错误类型参数
var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max write batch num")
	ErrMergeInProgress        = errors.New("merge is in progress, try again later")
	ErrWriteBatchCannotUse    = errors.New("cannot use write batch, no seq no file")
	ErrDatabaseIsUsing        = errors.New("database directory is using by another process")
)

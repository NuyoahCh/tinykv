package tinykv

import (
	"encoding/binary"
	"fmt"
	"github.com/Nuyoahch/tinykv/data"
	"sync"
	"sync/atomic"
)

// 非事务写入使用的序列号常量
const nonTransactionSeqNo = 0

// 事务完成标记使用的 key
var txnFinKey = []byte("txn-fin")

// WriteBatch 批量写数据
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

// NewWriteBatch 初始化 WriteBatch 结构体
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree &&
		!db.seqFileExists &&
		!db.initial {
		panic(fmt.Sprintf("cannot use write batch: %v", ErrWriteBatchCannotUse))
	}
	return &WriteBatch{
		options:       options,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 写入操作
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除操作
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		// 先判断是否存在 nil
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交操作
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 开始写数据
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeq 将事务序列号 seqNo 和真实 key 打包成一个新的 key： [seqNo 的变长编码字节][原始 key 字节]
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	// 先用 varint 把 seqNo 编成字节
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	// 前面放 seqNo，后面接原始 key
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n]) // 写入 seqNo
	copy(encKey[n:], key)     // 写入真实 key

	return encKey
}

// parseLogRecordKey 从上面打包后的 key 中解析出：真实 key 和 seqNo。 约定格式： [seqNo 的变长编码字节][原始 key 字节]
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	// 先把前面的 varint 解成 seqNo，返回值 n 是占用的字节数
	seqNo, n := binary.Uvarint(key)
	// 剩下的就是用户真实的 key
	realKey := key[n:]
	return realKey, seqNo
}

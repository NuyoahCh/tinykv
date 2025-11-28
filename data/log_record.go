package data

import "encoding/binary"

type LogRecordType = byte

// 初始化相关类型参数
const (
	// LogRecordNormal 正常的类型
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted 针对数据删除的类型
	LogRecordDeleted
)

// crc type keySize valueSize -> 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存索引，描述数据在磁盘位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存储的文件标识
	Offset int64  // 位置偏移，表示数据存储文件的位置
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 对字节数组中的 Handler 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

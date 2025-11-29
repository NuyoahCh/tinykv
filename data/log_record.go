package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// crc 在前面计算完毕之后进行操作，因为 crc 所占空间为 4 个字节，所以从第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5

	// 5 字节之后，存储 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// 编码后的长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	// 最终目标返回值
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 小段续的方式
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// 返回对应长度
	return encBytes, int64(size)
}

// 对字节数组中的 Handler 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// 小于 4 就是不符合要求
	if len(buf) <= 4 {
		return nil, 0
	}
	// 初始化参数
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	// 索引位置
	var index = 5
	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	// 下次读取的位置存储
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 校验 CRC 数值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}

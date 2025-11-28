package data

import (
	"errors"
	"fmt"
	"github.com/Nuyoahch/tinykv/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

// DataFileNameSuffix data 文件后缀常量
const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 完整的文件名称
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 初始化数据文件
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取到当前文件大小
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，这只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	// 如果已经超过了文件的长度，不然会产生错误
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 Handler 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 从数据文件当中读取完毕
	if header == nil {
		// 返回 EOF 错误
		return nil, 0, io.EOF
	}
	// 判断后也读取文件模块
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	// 记录 recordSize 长度
	var recordSize = headerSize + keySize + valueSize

	// 定义 logRecord 结构体
	logRecord := &LogRecord{Type: header.recordType}

	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		// 就是用户实际存储的数据
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 解出 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	// 有效信息，返回结果
	return logRecord, recordSize, nil
}

// Write 文件写入方法
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// Sync 持久化文件操作
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close 关闭文件
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// 对应读取多少字节，进行返回
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	// 读取长度
	b = make([]byte, n)
	// 进行读取
	_, err = df.IoManager.Read(b, offset)
	return
}

package data

import "github.com/Nuyoahch/tinykv/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// ReadLogRecord 读取数据日志记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

// Write 文件写入方法
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 持久化文件操作
func (df *DataFile) Sync() error {
	return nil
}

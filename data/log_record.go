package data

// LogRecordPos 数据内存索引，描述数据在磁盘位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存储的文件标识
	Offset int64  // 位置偏移，表示数据存储文件的位置
}

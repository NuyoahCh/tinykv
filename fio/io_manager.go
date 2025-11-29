package fio

// DataFilePerm 数据文件访问常量
const DataFilePerm = 0644

// FileIOType 文件 IO 类型
type FileIOType = byte

// IO 操作类型参数
const (
	StandardFile FileIOType = iota
	MemoryMap
)

// IOManager 抽象 IO 管理接口，可接入不同的 IO 类型
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFile:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}

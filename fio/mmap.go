package fio

import "golang.org/x/exp/mmap"

// MMap (Memory Map a File) IO 类型
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO 类型
func NewMMapIOManager(fileName string) (*MMap, error) {
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// 读取
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// 写入
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync 持久化
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 关闭
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取长度
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}

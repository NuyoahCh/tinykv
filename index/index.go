package index

import "github.com/Nuyoahch/tinykv/data"

// Indexer 抽象索引接口，后续接入其他的数据结构，可直接实现这个接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

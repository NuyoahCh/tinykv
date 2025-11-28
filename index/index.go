package index

import (
	"bytes"
	"github.com/Nuyoahch/tinykv/data"
	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续接入其他的数据结构，可直接实现这个接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

// IndexType 索引类型枚举
type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo 待实现 ART 自适应基数树索引
		return nil
	default:
		// panic 返回
		panic("unsupported index type")
	}
}

// Item 自定义 google btree 中 Item 结构
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 实现比较规则的方法
func (ai *Item) Less(bi btree.Item) bool {
	// 实现比较规则
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

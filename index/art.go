package index

import (
	"bytes"
	"github.com/Nuyoahch/tinykv/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
// 封装了 github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 新建 ART 索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 数据写入操作
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

// Get 根据 key 取出对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	// 调用底层查找实现
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete 根据 key 删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// Size 索引中的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Close 关闭操作
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	iterator := newArtIterator(art.tree, reverse)
	art.lock.RUnlock()
	return iterator
}

// ART 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key + 位置索引信息
}

// 初始化 ART 索引迭代器
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (ai *artIterator) Next() {
	ai.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的 Key 数据
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	ai.values = nil
}

package index

import (
	"github.com/Nuyoahch/tinykv/data"
	"github.com/google/btree"
	"sync"
)

// BTree 封装 google 实现 btree 库
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree  // btree 结构
	lock *sync.RWMutex // 加锁保护
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		// 初始化叶子结点的数量，可根据业务进行选择
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的数据位置信息
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	// 存储数据前保证并发安全，进行加锁
	bt.lock.Lock()
	// 将给定项添加到树中
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

// Get 根据 key 取出对应的索引位置信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	// 进行数据转换
	return btreeItem.(*Item).pos
}

// Delete 根据 key 删除对应的索引位置信息
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	// 删除操作
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}

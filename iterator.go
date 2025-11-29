package tinykv

import (
	"bytes"
	"github.com/Nuyoahch/tinykv/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator  // 索引迭代器
	db        *DB             // db 操作
	options   IteratorOptions // 迭代器配置
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// skipToNext 将迭代器移动到“下一个满足前缀条件的 key ”上
func (it *Iterator) skipToNext() {
	// 取出用户在迭代器选项中设置的前缀长度。
	// 如果 prefix 长度为 0，说明没有设置任何前缀过滤条件，
	// 此时不需要做额外的跳过操作，直接返回即可。
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	// 从当前 indexIter 所在位置开始，向后遍历：
	// - it.indexIter.Valid() 用来判断当前迭代器是否还在有效范围内；
	// - it.indexIter.Next() 在每次循环后向后移动一位。
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		// 取出当前索引迭代器指向的 key。
		key := it.indexIter.Key()

		// 如果当前 key 的长度小于前缀长度，那它肯定不可能以该前缀开头，
		// 直接跳过（继续下一轮循环）。
		// 如果当前 key 的长度 >= 前缀长度，再去比较前缀是否相等。
		if prefixLen <= len(key) &&
			// 使用 bytes.Compare 比较：
			//   - 返回 0 表示相等，
			bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {

			// 找到第一个满足前缀条件的 key 了，直接 break。
			// 此时 indexIter 将停留在这个 key 上，供外部使用。
			break
		}
	}

	// 如果循环结束后没有 break（即没找到满足条件的 key），
	// 那么 indexIter.Valid() 很可能已经为 false，
	// 表示已经遍历到末尾，后续调用方会根据 Valid() 判断迭代是否结束。
}

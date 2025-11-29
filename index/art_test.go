package index

import (
	"github.com/Nuyoahch/tinykv/data"
	"testing"
)

// 插入
func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})

	art.Put(nil, nil)
	t.Log(art.Size())
}

// 获取
func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("caas"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("eeda"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("bbue"), &data.LogRecordPos{Fid: 11, Offset: 123})

	val := art.Get([]byte("caas"))
	t.Log(val)
}

// 删除
func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	art.Put([]byte("caas"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("eeda"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("bbue"), &data.LogRecordPos{Fid: 11, Offset: 123})

	b := art.Delete([]byte("e1eda"))
	t.Log(b)
}

// 迭代器
func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("annde"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("cnedc"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("aeeue"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("esnue"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("bnede"), &data.LogRecordPos{Fid: 11, Offset: 123})

	art.Iterator(true)
}

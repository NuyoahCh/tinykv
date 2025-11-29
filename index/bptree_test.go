package index

import (
	"go.etcd.io/bbolt"
	"testing"
)

func TestBPlusTree_Iterator(t *testing.T) {
	db, _ := bbolt.Open("/tmp/bbolt", 0644, nil)
	db.Update(func(tx *bbolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte("name"))
		bucket.Put([]byte("bbccde"), []byte("b1"))
		bucket.Put([]byte("cchune"), []byte("b1"))
		bucket.Put([]byte("bbcaed"), []byte("b1"))
		bucket.Put([]byte("aacded"), []byte("b1"))
		bucket.Put([]byte("ccdeas"), []byte("b1"))
		return nil
	})

	db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("name"))
		cursor := bucket.Cursor()
		k, _ := cursor.Seek([]byte("bb"))
		t.Log(string(k))
		return nil
	})
}

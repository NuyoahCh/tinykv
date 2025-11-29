package tinykv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-get")
	dir := "/tmp/bitcask-go-merge-1"
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//for i := 0; i < 500000; i++ {
	//	db.Put(utils.GetTestKey(i), utils.RandomValue(128))
	//}
	//for i := 0; i < 500000; i++ {
	//	if i == 99033 {
	//		db.Put(utils.GetTestKey(i), utils.RandomValue(128))
	//	} else {
	//		db.Delete(utils.GetTestKey(i))
	//	}
	//}
	//
	//db.Merge()
}

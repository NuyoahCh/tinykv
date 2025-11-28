package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDir = "docs/test"

// 每个测试要用到的路径统一从这里拿，只使用相对路径。
func newTestPath(t *testing.T, filename string) string {
	t.Helper()

	// 确保 testdata/fio 目录存在（相对路径）
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("create test dir %s failed: %v", testDir, err)
	}

	return filepath.Join(testDir, filename) // 比如 "testdata/fio/a.data"
}

// 清理整个测试目录（相对路径）
func destroyTestData() {
	_ = os.RemoveAll(testDir)
}

// 创建目录
func TestNewFileIOManager(t *testing.T) {
	path := newTestPath(t, "a.data")
	defer destroyTestData()

	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

// 写入
func TestFileIO_Write(t *testing.T) {
	path := newTestPath(t, "write.data")
	defer destroyTestData()

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("tiny kv"))
	// 这里你的实现如果有额外编码（比如长度、CRC 等），自己改期望值。
	// 下面先按“写入多少字节就返回多少”来写：
	assert.Equal(t, len([]byte("tiny kv")), n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, len([]byte("storage")), n)
	assert.Nil(t, err)
}

// 读取
func TestFileIO_Read(t *testing.T) {
	path := newTestPath(t, "read.data")
	defer destroyTestData()

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

// 持久化数据
func TestFileIO_Sync(t *testing.T) {
	path := newTestPath(t, "sync.data")
	defer destroyTestData()

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

// 关闭文件
func TestFileIO_Close(t *testing.T) {
	path := newTestPath(t, "close.data")
	defer destroyTestData()

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

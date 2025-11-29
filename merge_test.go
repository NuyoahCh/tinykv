package tinykv

import (
	"os"
	"path"
	"testing"
)

func TestDB_Merge(t *testing.T) {
	p := "/tmp/a/b"

	//p = path.Clean(p)
	t.Log(path.Dir(p))
	t.Log(path.Base(p))

	entries, _ := os.ReadDir("/tmp/a")
	for _, entry := range entries {
		t.Log(entry.Name())
	}
}

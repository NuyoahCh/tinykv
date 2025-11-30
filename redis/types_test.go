package redis

import (
	"bytes"
	"testing"
)

func TestNewRedisDataStructure(t *testing.T) {
	b1 := []byte{3, 12, 99}
	b2 := []byte{4}

	t.Log(bytes.Compare(b1, b2))
}

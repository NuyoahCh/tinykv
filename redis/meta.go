package redis

import (
	"encoding/binary"
	"strconv"
)

const (
	maxMetadataSize = 21
	scorePrefix     = "$%!score$%!"
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += 16
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, 8+len(hk.key)+len(hk.field))
	var index = 0

	// key
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, 8+8+len(lk.key))

	var index = 0

	// key
	copy(buf[index:], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

// key + version + member
func (zk *zsetInternalKey) encodeMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	var index = 0

	// key
	copy(buf[index:], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

// key + version + score
func (zk *zsetInternalKey) encodeScore() []byte {
	scoreBuf := []byte(strconv.FormatFloat(zk.score, 'f', -1, 64))
	buf := make([]byte, len(scorePrefix)+len(zk.key)+len(scoreBuf)+8)

	var index = 0

	// prefix
	copy(buf[index:], scorePrefix)
	index += len(scorePrefix)

	// key
	copy(buf[index:], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:], scoreBuf)

	return buf
}

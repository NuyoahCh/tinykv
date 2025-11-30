package redis

import (
	"encoding/binary"
)

const maxMetadataSize = 21

const (
	metaPart = "$%!meta!%$"
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

func keyWithMetadata(key []byte) []byte {
	metaLen := len(key) + len(metaPart)
	metaKey := make([]byte, metaLen)
	copy(metaKey[:metaLen], metaPart)
	copy(metaKey[metaLen:], key)
	return metaKey
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

// type + version + key size + key + field
//
//	1   +    8    +    4     +  n  +  n
func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, 1+8+4+len(hk.key)+len(hk.field))

	// type
	buf[0] = Hash
	var index = 1

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// key size
	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(hk.key)))
	index += 4

	// key
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// field
	copy(buf[index:], hk.field)

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

// type + version + key + index
//
//	1   +    8    +  n  +  8
func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, 1+8+8+len(lk.key))

	// type
	buf[0] = List
	var index = 1

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// key
	copy(buf[index:], lk.key)
	index += len(lk.key)

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

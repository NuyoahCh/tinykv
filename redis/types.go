package redis

import (
	"encoding/binary"
	"fmt"
	"github.com/Nuyoahch/tinykv"
	"math"
	"strconv"
	"time"
)

type RedisDataStructure struct {
	db *tinykv.DB // 存储数据和元数据信息
}

type redisDataType = byte

const (
	String redisDataType = iota + 1
	List
	Hash
	Set
	ZSet
)

// NewRedisDataStructure 新建 Redis 数据结构服务
func NewRedisDataStructure(options tinykv.Options) (*RedisDataStructure, error) {
	db, err := tinykv.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// ====== String 数据结构 ======

// Set 设置一个 String 的值
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value : type + expire + payload
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	// 解码 value
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

// ====== Hash 数据结构 ======

// HSet 新增一个 Hash 的值
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查找对应的元数据信息
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != tinykv.ErrKeyNotFound {
		return false, err
	}

	var metaInfo *metadata
	var exist = true
	// 如果元数据信息没找到或已过期，则初始化
	if err == tinykv.ErrKeyNotFound {
		exist = false
	} else {
		metaInfo = decodeMetadata(metaBuf)
		if metaInfo.expire > 0 && metaInfo.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		metaInfo = &metadata{
			dataType: Hash,
			version:  time.Now().UnixNano(),
		}
	}

	// 构造 Internal Key
	// key + version + field
	internalKey := &hashInternalKey{
		key:     key,
		version: metaInfo.version,
		field:   field,
	}

	// 先查找数据是否存在
	var overwritten = true
	_, err = rds.db.Get(internalKey.encode())
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			overwritten = false
		} else {
			return false, err
		}
	}

	// 开始写数据
	wb := rds.db.NewWriteBatch(tinykv.DefaultWriteBatchOptions)
	// 写元数据
	if !overwritten {
		metaInfo.size++
		_ = wb.Put(key, metaInfo.encode())
	}
	// 写数据
	_ = wb.Put(internalKey.encode(), value)

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return overwritten, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	// 先查找对应的元数据信息
	meta, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	metaInfo := decodeMetadata(meta)
	if metaInfo.expire != 0 && metaInfo.expire <= time.Now().UnixNano() {
		return nil, nil
	}
	internalKey := &hashInternalKey{
		key:     key,
		version: metaInfo.version,
		field:   field,
	}

	// 再去查找数据
	value, err := rds.db.Get(internalKey.encode())
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return value, nil
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	// 先查找对应的元数据信息
	meta, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}

	metaInfo := decodeMetadata(meta)
	internalKey := &hashInternalKey{
		key:     key,
		version: metaInfo.version,
		field:   field,
	}

	var exists = true
	_, err = rds.db.Get(internalKey.encode())
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			exists = false
		} else {
			return false, err
		}
	}

	if exists {
		if err = rds.db.Delete(internalKey.encode()); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// ====== List 数据结构 ======

func (rds *RedisDataStructure) LPush(key []byte, value []byte) (uint32, error) {
	// 先查找对应的元数据信息
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != tinykv.ErrKeyNotFound {
		return 0, err
	}

	var metaInfo *metadata
	var exist = true
	// 如果元数据信息没找到或已过期，则初始化
	if err == tinykv.ErrKeyNotFound {
		exist = false
	} else {
		metaInfo = decodeMetadata(metaBuf)
		if metaInfo.expire > 0 && metaInfo.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		metaInfo = &metadata{
			dataType: List,
			version:  time.Now().UnixNano(),
			head:     math.MaxUint64 / 2,
			tail:     math.MaxUint64 / 2,
		}
	}

	internalKey := &listInternalKey{
		key:     key,
		version: metaInfo.version,
		index:   metaInfo.head - 1,
	}

	var size = metaInfo.size
	wb := rds.db.NewWriteBatch(tinykv.DefaultWriteBatchOptions)

	// 写元数据
	metaInfo.size++
	metaInfo.head--
	_ = wb.Put(key, metaInfo.encode())

	// 写数据
	_ = wb.Put(internalKey.encode(), value)

	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return size, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	// 先查找对应的元数据信息
	meta, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	metaInfo := decodeMetadata(meta)
	if metaInfo.expire != 0 && metaInfo.expire <= time.Now().UnixNano() {
		return nil, nil
	}

	if metaInfo.size <= 0 {
		return nil, nil
	}

	internalKey := &listInternalKey{
		key:     key,
		version: metaInfo.version,
		index:   metaInfo.head,
	}

	value, err := rds.db.Get(internalKey.encode())
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	wb := rds.db.NewWriteBatch(tinykv.DefaultWriteBatchOptions)

	// 元数据
	metaInfo.size--
	metaInfo.head++
	_ = wb.Put(key, metaInfo.encode())

	// 删除数据
	_ = wb.Delete(internalKey.encode())

	if err = wb.Commit(); err != nil {
		return nil, err
	}

	return value, nil
}

// ====== ZSet 数据结构 ======

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	// 先查找对应的元数据信息
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != tinykv.ErrKeyNotFound {
		return false, err
	}

	var metaInfo *metadata
	var exist = true
	// 如果元数据信息没找到或已过期，则初始化
	if err == tinykv.ErrKeyNotFound {
		exist = false
	} else {
		metaInfo = decodeMetadata(metaBuf)
		if metaInfo.expire > 0 && metaInfo.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		metaInfo = &metadata{
			dataType: ZSet,
			version:  time.Now().UnixNano(),
		}
	}

	internalKey := &zsetInternalKey{
		key:     key,
		version: metaInfo.version,
		member:  member,
		score:   score,
	}

	// 查找旧的值是否存在
	var scoreExist = true
	if _, err = rds.db.Get(internalKey.encodeMember()); err != nil {
		if err == tinykv.ErrKeyNotFound {
			scoreExist = false
		} else {
			return false, err
		}
	}

	wb := rds.db.NewWriteBatch(tinykv.DefaultWriteBatchOptions)
	// 存元数据
	if !scoreExist {
		metaInfo.size++
		_ = wb.Put(key, metaInfo.encode())
	}

	// 存 key + member -> score
	scoreBuf := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	_ = wb.Put(internalKey.encodeMember(), scoreBuf)

	// 存 key + score -> member
	_ = wb.Put(internalKey.encodeScore(), member)

	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !scoreExist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	// 先查找对应的元数据信息
	meta, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	metaInfo := decodeMetadata(meta)
	if metaInfo.expire != 0 && metaInfo.expire <= time.Now().UnixNano() {
		return 0, nil
	}
	if metaInfo.size <= 0 {
		return 0, nil
	}

	internalKey := &zsetInternalKey{
		key:     key,
		version: metaInfo.version,
		member:  member,
	}

	scoreBuf, err := rds.db.Get(internalKey.encodeMember())
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	return strconv.ParseFloat(string(scoreBuf), 64)
}

func (rds *RedisDataStructure) ZRangeByScore(key []byte, min, max float64) ([][]byte, error) {
	// 先查找对应的元数据信息
	meta, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	metaInfo := decodeMetadata(meta)
	if metaInfo.expire != 0 && metaInfo.expire <= time.Now().UnixNano() {
		return nil, nil
	}
	if metaInfo.size <= 0 {
		return nil, nil
	}

	internalKey := &zsetInternalKey{
		key:     key,
		version: metaInfo.version,
		score:   min,
	}

	options := tinykv.DefaultIteratorOptions
	options.Prefix = []byte(scorePrefix)
	iter := rds.db.NewIterator(options)
	defer iter.Close()
	var members [][]byte
	for iter.Seek(internalKey.encodeScore()); iter.Valid(); iter.Next() {
		fmt.Println("key = ", iter.Key())
		member, err := iter.Value()
		if err != nil {
			return nil, err
		}

		internalKey.member = member
		scoreBuf, err := rds.db.Get(internalKey.encodeMember())
		if err != nil {
			return nil, err
		}
		score, err := strconv.ParseFloat(string(scoreBuf), 64)
		if err != nil {
			return nil, err
		}
		if score > max {
			break
		}
		members = append(members, member)
	}
	return members, nil
}

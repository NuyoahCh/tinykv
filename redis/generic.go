package redis

import "github.com/Nuyoahch/tinykv"

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	metadata, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	metaInfo := decodeMetadata(metadata)
	return metaInfo.dataType, nil
}

func (rds *RedisDataStructure) Del(key []byte) (bool, error) {
	_, err := rds.db.Get(key)
	if err != nil {
		if err == tinykv.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}

	if err = rds.db.Delete(key); err != nil {
		return false, err
	}
	return true, nil
}

package redis

import (
	tinykv "github.com/Nuyoahch/tinykv"
	tinykv_redis "github.com/Nuyoahch/tinykv/redis"

	"path/filepath"
)

func main() {
	options := tinykv.DefaultOptions
	options.DirPath = filepath.Join("/tmp", "bitcask-go-redis")
	rds, err := tinykv_redis.NewRedisDataStructure(options)
	if err != nil {
		panic(err)
	}

	rds.Set([]byte("name"), 0, []byte("rose"))

	val, err := rds.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println("val = ", string(val))

	// rds.HSet([]byte("myset"), []byte("f1"), []byte("rose-hset"))
	ok, _ := rds.HDel([]byte("myset"), []byte("f1"))
	println("ok = ", ok)

	val2, err := rds.HGet([]byte("myset"), []byte("f1"))
	if err != nil {
		panic(err)
	}
	println("val2 = ", string(val2))

	// rds.LPush([]byte("mylist"), []byte("aaaaa"))
	// rds.LPush([]byte("mylist"), []byte("bbbb"))
	// rds.LPush([]byte("mylist"), []byte("ccc"))
	val3, err := rds.LPop([]byte("mylist"))
	if err != nil {
		panic(err)
	}
	println("val3 = ", string(val3))
}

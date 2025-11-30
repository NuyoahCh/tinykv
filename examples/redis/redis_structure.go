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

	rds.ZAdd([]byte("myzset"), 554, []byte("c"))
	rds.ZAdd([]byte("myzset"), 84, []byte("r"))
	rds.ZAdd([]byte("myzset"), 884, []byte("g"))
	rds.ZAdd([]byte("myzset"), 32, []byte("v"))

	rds.ZRange([]byte("myzset"))
}

package fio

import "os"

// FileIO 标准系统文件 IO 实现
type FileIO struct {
	fd *os.File // 系统文件描述符
}

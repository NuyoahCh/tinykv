package main

import (
	"fmt"
	"github.com/Nuyoahch/tinykv"
)

func main() {
	opts := tinykv.DefaultOptions
	opts.DirPath = "/tmp/tinykv"
	db, err := tinykv.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("tinykv"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}

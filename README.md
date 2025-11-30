# tinykv

A tiny key-value storage engine implemented in Go that demonstrates the core building blocks of log-structured KV stores. 这个项目用 Go 语言从零实现了一个迷你 KV 存储引擎，帮助你理解底层原理并快速上手实战。

## Features
- Append-only log-structured storage with background data file merging to reclaim space.
- Pluggable in-memory or on-disk indexes: B-Tree, Adaptive Radix Tree (ART), and disk-backed B+ Tree.
- Configurable write durability (sync on every write or periodic `BytesPerSync`).
- Support for transactional-style batch writes with sequence numbers to guarantee atomicity.
- Iterator with prefix and reverse scanning capabilities for ordered reads.
- Optional Redis protocol server for playing with the engine using existing Redis clients.

## Getting Started

### Prerequisites
- Go 1.24 or higher

### Installation
```bash
# clone repository
git clone https://github.com/Nuyoahch/tinykv.git
cd tinykv

# download dependencies
go mod download
```

### Quick Start
Use the built-in defaults to open a database and perform basic CRUD operations:
```go
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
    defer db.Close()

    _ = db.Put([]byte("name"), []byte("tinykv"))
    val, _ := db.Get([]byte("name"))
    fmt.Println("val =", string(val))
}
```
Run the example:
```bash
go run examples/basic_operation.go
```

### Redis Protocol Demo
Launch the Redis-compatible demo server (listens on `127.0.0.1:6380`) and talk to it with your favorite Redis client:
```bash
go run redis/cmd/server.go
```

### Configuration
Tune the engine via `Options`:
- `DirPath`: data directory path (default: system temp directory)
- `DataFileSize`: maximum size for each data file before rolling to a new one
- `SyncWrites`: force `fsync` on every write for stronger durability
- `IndexType`: choose `BTree`, `ART`, or `BPlusTree`
- `BytesPerSync`: periodically sync after the specified number of bytes written
- `MMapAtStartup`: load data files with memory-mapped IO on startup
- `DataFileMergeRatio`: threshold that triggers file compaction/merge

Iterators accept `IteratorOptions` for prefix filtering and reverse traversal, and `WriteBatchOptions` let you control batch size and sync behavior.

## Testing
Run the full test suite:
```bash
go test ./...
```

## Benchmarking
Micro-benchmark helpers live in the `benchmark/` directory. You can start with:
```bash
go test -bench=. ./benchmark/...
```

## Contributing
Issues and pull requests are welcome! Please include reproductions for bugs and add tests when introducing new functionality.

## License
A license file has not yet been provided. Please add an appropriate open-source license before distributing builds.

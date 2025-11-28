package tinykv

import (
	"github.com/Nuyoahch/tinykv/data"
	"github.com/Nuyoahch/tinykv/index"
	"sync"
)

// 存放面向用户的操作接口

// DB tiny kv 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex             // 并发访问安全，读写锁
	activeFile *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Put 写入 Key/Value 相关数据，Key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 有效 -> 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 拿到索引信息，追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		// 索引更新失败
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 Key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构当中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)

	// 如果 key 从内存数据结构中没找到，就说明 key 是不存在的
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	// 存在活跃的数据文件中
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		// 返回错误标识
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量 offset 读取对应数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断 logRecord 类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	// 实际返回数据
	return logRecord.Value, nil
}

// 追加写入到活跃数据文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 并发写入操作
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库没有写入时无文件生成，如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encodeRecord, size := data.EncodeLogRecord(logRecord)

	// 进行业务逻辑的判断，如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 现将当前活跃文件进行持久化，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前的活跃文件转化为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 记录写入的 offset
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodeRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息，进行返回
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// 设置当前活跃文件，在访问方法之前必须持有互斥锁
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	// 当前活跃文件不为空
	if db.activeFile != nil {
		// id 递增操作
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	// 传递数据文件
	db.activeFile = dataFile
	return nil
}

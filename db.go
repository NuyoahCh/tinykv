package tinykv

import (
	"errors"
	"github.com/Nuyoahch/tinykv/data"
	"github.com/Nuyoahch/tinykv/fio"
	"github.com/Nuyoahch/tinykv/index"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 存放面向用户的操作接口

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB tiny kv 存储引擎实例
type DB struct {
	options       Options
	mu            *sync.RWMutex             // 并发访问安全，读写锁
	fileIds       []int                     // 文件 id 只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile    *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles    map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index         index.Indexer             // 内存索引
	seqNo         uint64                    // 事务序列号，全局递增 atomic
	isMerging     bool                      // 是否正在 merge
	isInitial     bool                      // 是否是第一次初始化这个目录
	seqFileExists bool                      // seq 文件存在
	fileLock      *flock.Flock              // 文件锁
	bytesWrite    int                       // 当前累计写了多少个字节
}

// Open 打开 tiny kv 存储引擎实例方法
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial = false
	// 判断数据目录是否存在，如果不存在的话，就进行创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载 merge 文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+树不需要从文件中加载索引了
	if db.options.IndexType != BPlusTree {
		// 从 Hint 文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引的方法
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置数据文件的 IO 类型
		if db.options.MMapAtStartup {
			if err := db.resetDataFileIoType(); err != nil {
				return nil, err
			}
		}
	}

	if db.options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		_ = db.fileLock.Unlock()
	}()
	// 活跃文件为空
	if db.activeFile == nil {
		return nil
	}
	// 处理并发操作
	db.mu.Lock()
	defer db.mu.Unlock()

	// 写当前事务序列号到文件中
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	// 活跃文件为空
	if db.activeFile == nil {
		return nil
	}
	// 处理并发操作
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 写入 Key/Value 相关数据，Key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 有效 -> 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 拿到索引信息，追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecordWithLock(logRecord)
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

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// 写入到数据文件当中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 从内存索引中将其对应的 key 删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 Key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 处理并发操作
	db.mu.RLock()
	defer db.mu.RUnlock()

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

	// 从数据文件中获取 value 值
	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库中所有的 Key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	// 设置 key 的存储空间
	keys := make([][]byte, db.index.Size())
	var idx int
	// 遍历所有 key
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	// 返回 key 存储数组
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	// 并发操作
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// appendLogRecordWithLock 在持有 db.mu 互斥锁的前提下追加一条日志记录，保证写入操作的并发安全。
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 并发安全
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写入到活跃数据文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//// 并发写入操作，上面 appendLogRecordWithLock 进行处理
	//db.mu.Lock()
	//defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，因为数据库没有写入时无文件生成，如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
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
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 记录写入的 offset
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodeRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += int(size)
	// 根据用户配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息，进行返回
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// 设置当前活跃文件，在访问方法之前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// 当前活跃文件不为空
	if db.activeFile != nil {
		// id 递增操作
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFile)
	if err != nil {
		return err
	}

	// 传递数据文件
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	// 数据文件标识
	var fileIds []int
	// 遍历目录中的所有文件，找到所有以 .data 数据结尾的文件
	for _, entry := range dirEntries {
		// 后缀名结尾
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 0000001.data，取前面部分进行解析
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			// 追加写入
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，需要从小到大一次加载
	sort.Ints(fileIds)
	// 进行赋值
	db.fileIds = fileIds

	// 遍历每一个文件 id，打开对应的数据文件
	for i, fid := range fileIds {
		var ioType = fio.StandardFile
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		// 判断是否是活跃文件，最后一个 id 最大的，就是当前的活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			// 说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引，遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 初始化变量值
	hasMerge, nonMergeFileId := false, uint32(0)
	// 合并文件名称
	mergeFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	// 修改判断逻辑
	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// updateIndex 根据日志类型更新内存索引：
	//   - 普通记录：在索引中插入/更新 key -> logRecordPos
	//   - 删除记录：从索引中删除该 key
	// 如果更新失败（返回 false），说明索引实现出错，直接 panic。
	updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key) // 删除类型，从索引中移除 key
		} else {
			ok = db.index.Put(key, logRecordPos) // 其他类型，更新/插入索引
		}
		if !ok {
			panic("failed to update index while startup") // 启动恢复时索引更新失败，认为是致命错误
		}
	}

	// currentSeqNo 记录当前正在重放的事务序列号，默认是非事务（0）
	var currentSeqNo uint64 = nonTransactionSeqNo

	// transactionRecords 用于在启动时重放日志时，
	// 按事务序列号分组暂存该事务内的所有记录，
	// 等到确认事务完成（例如遇到 txn-fin 标记）后再一次性应用到索引。
	transactionRecords := make(map[uint64][]*data.TransactionRecord)

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		// 如果是当前的活跃文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			// 从旧的数据文件当中查找
			dataFile = db.olderFiles[fileId]
		}

		// 偏移量
		var offset int64 = 0
		// 循环处理文件当中的内容
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			// 不能直接进行返回
			if err != nil {
				// 都读完了就跳出循环
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引信息
			pos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析 key，取出事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			// 非事务写入使用的序列号常量
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 事务完成，可以更新到内存中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						// 更新内存索引
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 暂存数据
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    pos,
					})
				}
			}
			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			// 记录偏移
			db.activeFile.WriteOff = offset
		}
	}
	// 更新当前最新的序列号
	db.seqNo = currentSeqNo
	return nil
}

// 加载事务序列号
func (db *DB) loadSeqNo() error {
	// 获取文件名
	seqNoFileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(seqNoFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开文件
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqFileExists = true

	// 删除这个文件，避免一直追加写入
	return os.Remove(seqNoFileName)
}

// 检查传入配置项的校验
func checkOptions(options Options) error {
	// 目录文件为空
	if options.DirPath == "" {
		return errors.New("database dir is empty")
	}
	// 数据文件大小无效
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than zero")
	}
	return nil
}

// 重设文件 IO 类型
func (db *DB) resetDataFileIoType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewFileIOManager(data.GetDataFileName(db.options.DirPath, db.activeFile.FileId))
	if err != nil {
		return err
	}
	db.activeFile.IoManager = ioManager
	for _, file := range db.olderFiles {
		if err := file.IoManager.Close(); err != nil {
			return err
		}
		ioManager, err := fio.NewFileIOManager(data.GetDataFileName(db.options.DirPath, file.FileId))
		if err != nil {
			return err
		}
		file.IoManager = ioManager
	}
	return nil
}

package tinykv

import (
	"github.com/Nuyoahch/tinykv/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

// merge 相关变量
const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 合并操作
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 正在执行 merge 操作
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeInProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 取出所有需要 merge 的文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 没有被 merge 的文件 id
	nonMergeFileId := db.activeFile.FileId + 1
	// 已经被 merge 的文件 id 放到旧文件 map 中进行存储
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	//	打开一个新的数据文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// merge 文件列表
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 待 merge 的文件从小到大进行排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergeDirPath()
	// 目录存在则删除
	if _, err := os.Stat(mergePath); os.IsExist(err) {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开新的 DB
	mergeOptions := db.options
	mergeOptions.SyncWrites = false
	mergeOptions.DirPath = mergePath
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	hintFile, err := data.OpenHintFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 写 Hint 索引信息
				if err := hintFile.WriteHintRecord(logRecord.Key, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinFile, err := data.OpenMergeFinishedFile(mergeDB.options.DirPath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}
	return nil
}

// 获取合并文件路径
func (db *DB) getMergeDirPath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 加载合并文件
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergeDirPath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	//	遍历查找是否完成了 merge
	var mergeFinished bool
	var fileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		fileNames = append(fileNames, entry.Name())
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//	先删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if err := os.Remove(fileName); err != nil {
			return err
		}
	}

	// 移动文件
	for _, fileName := range fileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

// 加载索引文件
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)
		offset += size
	}
	return nil
}

// 获取没有合并文件 id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

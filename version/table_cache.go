package version

import (
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/sstable"
)

type TableCache struct {
	mu     sync.Mutex
	dbName string
	cache  *lru.Cache
}

// sstable直接缓存到内存，一个文件4MB，缓存990个
func NewTableCache(dbName string) *TableCache {
	var tableCache TableCache
	tableCache.dbName = dbName
	tableCache.cache, _ = lru.New(internal.MaxOpenFiles - internal.NumNonTableCacheFiles)
	return &tableCache
}

// 迭代查询sstable里面的内容
func (tableCache *TableCache) NewIterator(fileNum uint64) *sstable.Iterator {
	table, _ := tableCache.findTable(fileNum)
	if table != nil {
		return table.NewIterator()
	}
	return nil
}

//通过缓存中查sstable数据，如果没有先读后加入
func (tableCache *TableCache) Get(fileNum uint64, key []byte) ([]byte, error) {
	table, err := tableCache.findTable(fileNum)
	if table != nil {
		return table.Get(key)
	}

	return nil, err
}

//删除缓存
func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.cache.Remove(fileNum)
}

//查数据
func (tableCache *TableCache) findTable(fileNum uint64) (*sstable.SsTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()
	table, ok := tableCache.cache.Get(fileNum)
	if ok {
		return table.(*sstable.SsTable), nil
	} else {
		ssTable, err := sstable.Open(internal.TableFileName(tableCache.dbName, fileNum))
		tableCache.cache.Add(fileNum, ssTable)
		return ssTable, err
	}
}

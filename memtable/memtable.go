package memtable

import (
	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/skiplist"
)

type MemTable struct {
	table       *skiplist.SkipList
	memoryUsage uint64
}

// New 构造memtable
func New() *MemTable {
	var memTable MemTable
	memTable.table = skiplist.New(internal.InternalKeyComparator) //构造实现memtable的跳表结构出来
	return &memTable
}

func (memTable *MemTable) NewIterator() *Iterator {
	return &Iterator{listIter: memTable.table.NewIterator()}
}

func (memTable *MemTable) Add(seq uint64, valueType internal.ValueType, key, value []byte) {
	internalKey := internal.NewInternalKey(seq, valueType, key, value)

	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) ([]byte, error) {
	//这里输入进来的key为user key，需要进行转换为internal key才能进行数据的查询操作
	lookupKey := internal.LookupKey(key) //lookup key就是internal key，通过用户输入的user key来内部计算出来

	it := memTable.table.NewIterator() //构造迭代器，iterator，通过迭代器进行memtable的查询操作
	it.Seek(lookupKey)                 //从memtable（跳表）去进行seek操作
	if it.Valid() {                    //判断node是否合法（这里只简单校验一下node是否为空）
		internalKey := it.Key().(*internal.InternalKey)
		if internal.UserKeyComparator(key, internalKey.UserKey) == 0 { //等于0说明key和internalKey中的userKey相等
			// 判断valueType
			if internalKey.Type == internal.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, internal.ErrDeletion
			}
		}
	}
	return nil, internal.ErrNotFound
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}

func (memTable *MemTable) GetMem() *skiplist.SkipList {
	return memTable.table
}

package db

import (
	"log"
	"sync"

	"time"

	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/version"
)

type Db struct {
	name                  string
	mu                    sync.Mutex
	cond                  *sync.Cond
	mem                   *memtable.MemTable
	imm                   *memtable.MemTable
	current               *version.Version
	bgCompactionScheduled bool
}

func Open(dbName string) *Db {
	var db Db
	db.name = dbName
	db.mem = memtable.New()
	db.imm = nil
	db.bgCompactionScheduled = false
	db.cond = sync.NewCond(&db.mu)
	// 最新一次的MANIFEST文件号
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := version.Load(dbName, num)
		if err != nil {
			return nil
		}
		db.current = v
	} else {
		db.current = version.New(dbName)
	}

	return &db
}

func (db *Db) Close() {
	db.mu.Lock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
	db.mu.Unlock()
}

func (db *Db) Put(key, value []byte) error {
	// May temporarily unlock and wait.
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// todo : add log

	db.mem.Add(seq, internal.TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mem := db.mem //memtable
	imm := db.mem //immetable
	current := db.current
	db.mu.Unlock()

	//从memtable中去读取
	value, err := mem.Get(key)
	if err != internal.ErrNotFound {
		return value, err
	}

	//从imm中读取数据
	if imm != nil {
		value, err := imm.Get(key)
		if err != internal.ErrNotFound {
			return value, err
		}
	}

	value, err = current.Get(key)
	return value, err
}

func (db *Db) Delete(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, internal.TypeDeletion, key, nil)
	return nil
}

// 写入速度下降的case：
//
//	当0层sstable文件多余8个时候，用户写会被降低；
//
// 写入被限制的case：
//
//	当0层sstable文件大于12个停止写入；
//	mem超过阈值转为imm，imm未持久化到sstable停止写入
//
// 触发合并的两个case:
//
//	0层超过4个文件开始合并
//	其他层数据库超过层级最大值开始合并
//
// 其他性能上的限制：
//
//	加锁，导致写只能串行；
//	cond引入导致可以写，但是提交时间会变长（返回时间变长）
//	其他场景通过内存拷本副本方式，降低block时间
func (db *Db) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for true {
		if db.current.NumLevelFiles(0) >= internal.L0_SlowdownWritesTrigger { //如果sst第0层的文件数大于L0_SlowdownWritesTrigger的话，就降低写入速度
			// L0超过8个文件就写的慢一点，后台merge跟不上，并且L0文件之间是无序的
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() <= internal.Write_buffer_size {
			// memtable的内存大小如果达到了阈值(Write_buffer_size)的话，就新建一个memtable出来，并且把这个memtable转换为immutable
			// mem还没达到阈值，可以继续写
			return db.current.NextSeq(), nil
		} else if db.imm != nil {
			// imm还没持久化到文件，不可写。此处可以优化成可以继续写，当mem满了且imm没持久化完成时在限制写入
			db.cond.Wait()
		} else {
			// mem达到阈值，且没有imm时候，需要持久化到sstable
			db.imm = db.mem
			db.mem = memtable.New()      //新建一个memtable出来
			db.maybeScheduleCompaction() //触发sst文件的compaction操作
		}
	}

	return db.current.NextSeq(), nil
}

func (db *Db) PrintMem() {
	log.Printf("memory total = %dB\n", db.mem.ApproximateMemoryUsage())
	log.Printf("\n" + db.mem.GetMem().Print())
	log.Println()
}

func (db *Db) PrintVersion() {
	log.Printf("\n" + db.current.Print())
	log.Println()
}

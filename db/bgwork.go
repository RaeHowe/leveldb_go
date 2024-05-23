package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/merlin82/leveldb/internal"
)

func (db *Db) maybeScheduleCompaction() {
	if db.bgCompactionScheduled { // 最多只发起一个后台协程来写数据
		return //如果compaction正在被调度的话就直接返回，先不进行compaction操作
	}
	db.bgCompactionScheduled = true //进行compaction操作
	go func() {
		db.mu.Lock()
		defer db.mu.Unlock()
		db.backgroundCompaction()
		db.bgCompactionScheduled = false
		db.cond.Broadcast()
	}()
}

// https://wingsxdu.com/post/database/leveldb/#tablecache
func (db *Db) backgroundCompaction() {
	// 先复制一份，然后就可以释放锁，用户可以继续写。但是提交会卡主，需要等到imm完全写到文件后释放。
	imm := db.imm                // 可以不用深拷贝，因为imm未刷盘时不会有新的imm生成 TODO 是这个意思?
	version := db.current.Copy() // 需要深拷贝，虽然不会有新的sstable生成，但是version字段会更新，如果有查询操作会出现问题 TODO 是这个意思?
	//db.mu.Unlock()

	// minor compaction：写imm到sstable，L0文件之间是没有关系的。
	// 如果发现sstable可以属于L1的sstable子集，优先向下合并。
	if imm != nil {
		version.WriteLevel0Table(imm) //flush过程。把文件内容放置到version的文件集合里面
	}
	// major compaction：合并，L1之后的sstable文件之前是单调增的
	for version.DoCompactionWork() { //里面会判断是否需要进行sst的compact，如果需要，则一直进行这个for循环操作
		// 每次合并后打印下version信息，除了看，没啥用
		version.Log()
	}
	// 写新的MANIFEST文件信息，因为version信息已经变更，需要及时更新元信息
	descriptorNumber, _ := version.Save()
	// 更新CURRENT文件内容
	db.SetCurrentFile(descriptorNumber)
	//db.mu.Lock()
	db.imm = nil //imm中的数据flush到磁盘完毕之后，把imm清空
	db.current = version
}

// 更新current文件里面的值，为了保证原子操作，此处用mv来实现
func (db *Db) SetCurrentFile(descriptorNumber uint64) {
	tmp := internal.TempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(tmp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmp, internal.CurrentFileName(db.name))
}

func (db *Db) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(internal.CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}

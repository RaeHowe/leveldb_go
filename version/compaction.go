package version

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/sstable"
)

type Compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

//可以直接把文件下移
func (c *Compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

//记录合并log
func (c *Compaction) Log() {
	log.Printf("Compaction, level:%d", c.level)
	ss := ""
	for i := 0; i < len(c.inputs[0]); i++ {
		ss += fmt.Sprintf("%d ", c.inputs[0][i].number)
	}
	log.Printf("inputs[0]: %s\n", ss)

	ss = ""
	for i := 0; i < len(c.inputs[1]); i++ {
		ss += fmt.Sprintf("%d ", c.inputs[1][i].number)
	}
	log.Printf("inputs[1]: %s\n", ss)
}

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	binary.Write(w, binary.LittleEndian, meta.number)
	meta.smallest.EncodeTo(w)
	meta.largest.EncodeTo(w)
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, &meta.fileSize)
	binary.Read(r, binary.LittleEndian, &meta.number)
	meta.smallest = new(internal.InternalKey)
	meta.smallest.DecodeFrom(r)
	meta.largest = new(internal.InternalKey)
	meta.largest.DecodeFrom(r)
	return nil
}

//当前version信息写到MANIFEST-xxxx文件里面；
//记录内容为：
//	1.下一个文件的id
//	2.当前最新的lsn
//	文件层级关系
//		3.文件数
//		每个文件的原信息
//      	4.文件大小
//      	5.文件序号
//      	6.文件最大值
//      	7.文件最小值
func (v *Version) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, v.nextFileNumber)
	binary.Write(w, binary.LittleEndian, v.seq)
	for level := 0; level < internal.NumLevels; level++ {
		numFiles := len(v.files[level])
		binary.Write(w, binary.LittleEndian, int32(numFiles))

		for i := 0; i < numFiles; i++ {
			v.files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	binary.Read(r, binary.LittleEndian, &v.seq)
	var numFiles int32
	for level := 0; level < internal.NumLevels; level++ {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			meta.DecodeFrom(r)
			v.files[level][i] = &meta
		}
	}
	return nil
}

func (v *Version) deleteFile(level int, meta *FileMetaData) {
	numFiles := len(v.files[level])
	for i := 0; i < numFiles; i++ {
		if v.files[level][i].number == meta.number {
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...)
			log.Printf("deleteFile, level:%d, num:%d", level, meta.number)
			break
		}
	}
}

func (v *Version) addFile(level int, meta *FileMetaData) {
	log.Printf("addFile, level:%d, num:%d, %s-%s", level, meta.number, string(meta.smallest.UserKey), string(meta.largest.UserKey))
	if level == 0 {
		// 0层没有排序
		v.files[level] = append(v.files[level], meta)
	} else {
		numFiles := len(v.files[level])
		index := v.findFile(v.files[level], meta.smallest.UserKey)
		if index >= numFiles {
			v.files[level] = append(v.files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, v.files[level][:index]...)
			tmp = append(tmp, meta)
			v.files[level] = append(tmp, v.files[level][index:]...)
		}
	}
}

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
	var meta FileMetaData
	meta.allowSeeks = 1 << 30 // 1GB
	meta.number = v.nextFileNumber
	v.nextFileNumber++
	// sstable内存形式
	builder := sstable.NewTableBuilder((internal.TableFileName(v.tableCache.dbName, meta.number)))
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		// 先把imm写到内存，4k刷盘一次
		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
		}
		// 落盘； data(最后一块刷盘) + index + footer 三部分
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil
	}

	// 挑选合适的level
	level := 0
	if !v.overlapInLevel(0, meta.smallest.UserKey, meta.largest.UserKey) {
		for ; level < internal.MaxMemCompactLevel; level++ {
			if v.overlapInLevel(level+1, meta.smallest.UserKey, meta.largest.UserKey) {
				break
			}
		}
	}

	// 因为imm已经写到文件，version维护的sstable信息需要更新
	v.addFile(level, &meta)
}

func (v *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(v.files[level])
	if numFiles == 0 {
		return false
	}
	if level == 0 {
		for i := 0; i < numFiles; i++ {
			f := v.files[level][i]
			if internal.UserKeyComparator(smallestKey, f.largest.UserKey) > 0 || internal.UserKeyComparator(f.smallest.UserKey, largestKey) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		index := v.findFile(v.files[level], smallestKey)
		if index >= numFiles {
			return false
		}
		if internal.UserKeyComparator(largestKey, v.files[level][index].smallest.UserKey) > 0 {
			return true
		}
	}
	return false
}

func (v *Version) DoCompactionWork() bool {
	// 选择要合并的level和sstable文件
	c := v.pickCompaction()
	if c == nil {
		return false
	}
	log.Printf("DoCompactionWork begin\n")
	defer log.Printf("DoCompactionWork end\n")

	// 打日志，merge的文件
	c.Log()

	// 先判断是否可以直接下移一层，如果有直接下移，都是内存操作，如果崩溃也没事
	// 判断时如果上一层是1个文件，下一层没有文件，可以直接下移
	if c.isTrivialMove() {
		v.deleteFile(c.level, c.inputs[0][0])
		v.addFile(c.level+1, c.inputs[0][0])
		return true
	}

	// 合并后生成的新文件
	var list []*FileMetaData
	var current_key *internal.InternalKey

	// sstable迭代器
	iter := v.makeInputIterator(c)

	// 从最小的sstable开始，每个行记录为维度向后merge
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.number = v.nextFileNumber
		v.nextFileNumber++

		// 合并后新的sstable文件名
		sstableFileName := internal.TableFileName(v.tableCache.dbName, meta.number)

		// 创建文件，并且申请一块内存用来缓存记录
		builder := sstable.NewTableBuilder(sstableFileName)

		// 要合并的sstable中最小的key
		meta.smallest = iter.InternalKey()

		// 归并排序
		//    大于4k刷盘一次，超过2MB切换到下一个文件，切换之前需要添加尾信息
		for ; iter.Valid(); iter.Next() {
			if current_key != nil {
				// 去除重复的记录
				ret := internal.UserKeyComparator(iter.InternalKey().UserKey, current_key.UserKey)
				if ret == 0 {
					log.Printf("%s == %s", string(iter.InternalKey().UserKey), string(current_key.UserKey))
					continue
				} else if ret < 0 {
					log.Fatalf("%s < %s", string(iter.InternalKey().UserKey), string(current_key.UserKey))
				}
			}
			current_key = iter.InternalKey()
			meta.largest = iter.InternalKey()

			// 4KB刷盘一次
			builder.Add(iter.InternalKey())

			// 单个sstable文件最大2MB，超过就添加新文件
			if builder.FileSize() > internal.MaxFileSize {
				break
			}
		}

		// 添加尾信息
		builder.Finish()

		// 新文件生成了，记录文件元信息
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil

		// 添加到新文件列表，因为后面要更新version
		list = append(list, &meta)
	}

	// 从version中删除level信息
	for i := 0; i < len(c.inputs[0]); i++ {
		v.deleteFile(c.level, c.inputs[0][i])
	}

	// 从version中删除level+1信息
	for i := 0; i < len(c.inputs[1]); i++ {
		v.deleteFile(c.level+1, c.inputs[1][i])
	}

	// 在version中添加新的level+1信息
	for i := 0; i < len(list); i++ {
		v.addFile(c.level+1, list[i])
	}

	return true
}

func (v *Version) makeInputIterator(c *Compaction) *MergingIterator {
	var list []*sstable.Iterator
	for i := 0; i < len(c.inputs[0]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[0][i].number))
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[1][i].number))
	}
	return NewMergingIterator(list)
}

// 选择最需要合并的level，
//    如果是L0
//       所有文件合并到L1中，
//    如果是L1以上
//       选择一个 Level-N 文件，找到所有和该 Level-N 有重复 Key 的 Level-(N+1) 文件进行合并。
func (v *Version) pickCompaction() *Compaction {
	var c Compaction
	// 根据文件大小、或者文件个数判断超出规定的level进行压缩
	c.level = v.pickCompactionLevel()
	if c.level < 0 {
		return nil
	}
	// 找最大key和最小key，为合并做准备
	var smallest, largest *internal.InternalKey
	if c.level == 0 {
		// L0的所有sstable文件
		c.inputs[0] = append(c.inputs[0], v.files[c.level]...)
		smallest = c.inputs[0][0].smallest
		largest = c.inputs[0][0].largest
		for i := 1; i < len(c.inputs[0]); i++ {
			f := c.inputs[0][i]
			if internal.InternalKeyComparator(f.largest, largest) > 0 {
				largest = f.largest
			}
			if internal.InternalKeyComparator(f.smallest, smallest) < 0 {
				smallest = f.smallest
			}
		}
	} else {
		// Pick the first file that comes after compact_pointer_[level]
		for i := 0; i < len(v.files[c.level]); i++ {
			f := v.files[c.level][i]
			if v.compactPointer[c.level] == nil || internal.InternalKeyComparator(f.largest, v.compactPointer[c.level]) > 0 {
				c.inputs[0] = append(c.inputs[0], f)
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], v.files[c.level][0])
		}
		smallest = c.inputs[0][0].smallest
		largest = c.inputs[0][0].largest
	}

	//选择一个 Level-N 文件，找到所有和该 Level-N 有重复 Key 的 Level-(N+1) 文件进行合并。
	for i := 0; i < len(v.files[c.level+1]); i++ {
		f := v.files[c.level+1][i]
		if internal.InternalKeyComparator(f.largest, smallest) < 0 || internal.InternalKeyComparator(f.smallest, largest) > 0 {
			// "f" is completely before specified range; skip it,  // "f" is completely after specified range; skip it
		} else {
			c.inputs[1] = append(c.inputs[1], f)
		}
	}
	return &c
}

// 选择超出最严重的先压缩
//    L0层比较文件个数，如果文件大于4个，进入候选
//    L1~L7层比较文件大小，如果大小大于规定值，进入候选
//    最后选择最严重的level进行压缩
func (v *Version) pickCompactionLevel() int {
	// We treat level-0 specially by bounding the number of files
	// instead of number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and
	// therefore we wish to avoid too many files when the individual
	// file size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := 0; level < internal.NumLevels-1; level++ {
		if level == 0 {
			score = float64(len(v.files[0])) / float64(internal.L0_CompactionTrigger)
		} else {
			score = float64(totalFileSize(v.files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}

	}
	return compactionLevel
}

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	for i := 0; i < len(files); i++ {
		sum += files[i].fileSize
	}
	return sum
}

// 每层文件大小按照10倍递增
// L0: 10MB
// L1: 10MB
// L2: 100MB
// L3: 1000MB = 1GB
// L4: 10000MB = 10GB
// L5: 100000MB = 100GB
// L6: 1000000MB = 1000GB = 1TB
// L7: 10000000MB = 10000GB = 10TB
func maxBytesForLevel(level int) float64 {
	// Note: the result for level zero is not really used since we set
	// the level-0 compaction threshold based on number of files.

	// Result for both level-0 and level-1
	result := 10. * internal.BaseLevelSize
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

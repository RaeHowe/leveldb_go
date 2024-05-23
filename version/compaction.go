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
	inputs [2][]*FileMetaData //二维数组，索引为1的数据代表需要进行compaction的sst
}

// 可以直接把文件下移
func (c *Compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

// 记录合并log
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

// 当前version信息写到MANIFEST-xxxx文件里面；
// 记录内容为：
//
//		1.下一个文件的id
//		2.当前最新的lsn
//		文件层级关系
//			3.文件数
//			每个文件的原信息
//	     	4.文件大小
//	     	5.文件序号
//	     	6.文件最大值
//	     	7.文件最小值
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
		numFiles := len(v.files[level])                            //第n(n>0)层的sst文件集合
		index := v.findFile(v.files[level], meta.smallest.UserKey) //找到这个key在第level层的第几个sst文件中
		if index >= numFiles {                                     //如果index大于文件数量了，直接append到最后即可
			v.files[level] = append(v.files[level], meta)
		} else { //如果index在文件中间
			var tmp []*FileMetaData
			//   smallest key file -- new file -- largest key file
			tmp = append(tmp, v.files[level][:index]...)            //append index前部分的所有文件
			tmp = append(tmp, meta)                                 //再append这个sst文件
			v.files[level] = append(tmp, v.files[level][index:]...) //再把剩余的文件append上
		}
	}
}

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
	var meta FileMetaData
	meta.allowSeeks = 1 << 30 // 1GB 扫描1G的数据
	meta.number = v.nextFileNumber
	v.nextFileNumber++
	// sstable内存形式
	builder := sstable.NewTableBuilder((internal.TableFileName(v.tableCache.dbName, meta.number)))
	iter := imm.NewIterator()
	iter.SeekToFirst() //获取到第一个key
	if iter.Valid() {
		// 先把imm写到内存，4k刷盘一次
		meta.smallest = iter.InternalKey() //第一个key，肯定是最小的
		for ; iter.Valid(); iter.Next() {
			meta.largest = iter.InternalKey() //把imm中的key依次加入到table builder里面去，key的最大值是动态更新的
			builder.Add(iter.InternalKey())
		}

		//遍历完imm中的key之后，把table builder进行落盘操作
		// 落盘； data(最后一块刷盘) + index + footer 三部分
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil
	}

	//刷完level0的sst之后，如果有必要从level0 compact到更高层，就继续compaction操作
	// 挑选合适的level
	level := 0
	if !v.overlapInLevel(0, meta.smallest.UserKey, meta.largest.UserKey) { //判断本次刷入到level0的sst文件是否和已经存在于level0中的sst文件存在key重叠的情况
		//如果不存在交集的话
		for ; level < internal.MaxMemCompactLevel; level++ {
			if v.overlapInLevel(level+1, meta.smallest.UserKey, meta.largest.UserKey) { //继续判断level1~n是否有交集，但凡一个有交集，就break
				break
			}
		}
	}

	// 因为imm已经写到文件，version维护的sstable信息需要更新
	v.addFile(level, &meta)
}

func (v *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(v.files[level]) //获取到指定level的文件个数
	if numFiles == 0 {              //如果没有文件直接返回
		return false
	}
	if level == 0 { //针对level0的情况
		for i := 0; i < numFiles; i++ { //遍历level0中的各个sst文件
			f := v.files[level][i] //获取到level0的第一个sst文件
			if internal.UserKeyComparator(smallestKey, f.largest.UserKey) > 0 || internal.UserKeyComparator(f.smallest.UserKey, largestKey) > 0 {
				//如果将要刷入level0的sst文件的最小key大于已经存在于level0的sst文件的最大值的话 || 如果将要刷入level0的sst文件的最大key小于已经存在于level0的sst文件的最小值
				//   old sst largest key <  new sst smallest key   ||    new sst largest key < old sst smallest key 这种情况就代表
				continue
			} else {
				//如果要写入的sst文件的key的最小值，小于已存在sst文件的最大值的话，肯定就会存在key的交集
				return true
			}
		}
	} else {
		index := v.findFile(v.files[level], smallestKey) //通过最小key获取到level1里面指定的sst文件(查找的过程是一个二分查找的过程)
		//index代表了v.file数组里面的第几个sst文件
		if index >= numFiles { //如果index大于这层level的文件个数的话，返回false，代表不存在重叠
			return false
		}
		if internal.UserKeyComparator(largestKey, v.files[level][index].smallest.UserKey) > 0 { //如果要写入文件的sst的key最大值大于已存在的sst的最小值的话，肯定会存在key的重叠
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
//
//	如果是L0
//	   所有文件合并到L1中，
//	如果是L1以上
//	   选择一个 Level-N 文件，找到所有和该 Level-N 有重复 Key 的 Level-(N+1) 文件进行合并。
func (v *Version) pickCompaction() *Compaction {
	var c Compaction
	// 根据文件大小、或者文件个数判断超出规定的level进行压缩
	c.level = v.pickCompactionLevel() //计算出来最需要进行compaction的level
	if c.level < 0 {                  //如果每层的score值都小于1.0的话，就不需要进行compaction
		return nil
	}
	// 找最大key和最小key，为合并做准备
	var smallest, largest *internal.InternalKey
	if c.level == 0 {
		// L0的所有sstable文件
		c.inputs[0] = append(c.inputs[0], v.files[c.level]...) //把level0的所有sst文件放置到compaction的inputs集合里面
		smallest = c.inputs[0][0].smallest                     //leve0的第一个sst文件的第一个key（最小key）
		largest = c.inputs[0][0].largest                       //leve0的第一个sst文件的最后一个key （一个sst文件里面的最大key）
		for i := 1; i < len(c.inputs[0]); i++ {                //遍历level0的所有sst文件
			f := c.inputs[0][i]
			if internal.InternalKeyComparator(f.largest, largest) > 0 { //如果第i个sst文件的最大key大于largest的话，代表有key的重叠，替换largest
				largest = f.largest
			}
			if internal.InternalKeyComparator(f.smallest, smallest) < 0 { //如果第i个sst文件的最小key小于smallest的话，代表有key的重叠，替换smallest
				smallest = f.smallest
			}
		}
	} else { //level 1~n
		// Pick the first file that comes after compact_pointer_[level]
		for i := 0; i < len(v.files[c.level]); i++ {
			f := v.files[c.level][i]
			if v.compactPointer[c.level] == nil || internal.InternalKeyComparator(f.largest, v.compactPointer[c.level]) > 0 { //找到开始进行compaction的位置
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
			//没有发生key的重叠不需要进行compaction
		} else {
			c.inputs[1] = append(c.inputs[1], f)
		}
	}
	return &c
}

// 选择超出最严重的先压缩
//
//	L0层比较文件个数，如果文件大于4个，进入候选
//	L1~L7层比较文件大小，如果大小大于规定值，进入候选
//	最后选择最严重的level进行压缩
func (v *Version) pickCompactionLevel() int {
	// We treat level-0 specially by bounding the number of files
	// instead of number of bytes for two reasons:
	// 我们通过限制文件数量而不是字节数来特别处理级别0，原因有两个:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (1) 对于较大的写缓冲区，最好不要进行太多的0级合并。（因为level0往level1的compaction过程是单线程的，如果写缓冲区较大，就不需要频繁的进行这个操作）
	//
	// (2) The files in level-0 are merged on every read and
	// therefore we wish to avoid too many files when the individual
	// file size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	//
	// (2) 0级的文件在每次读取时都会合并，因此我们希望在单个文件很小的情况下(可能是由于写缓冲区设置较小，或者是由于很高的压缩率，
	// 或者是由于大量的覆盖/删除)避免产生过多的文件。
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := 0; level < internal.NumLevels-1; level++ {
		if level == 0 {
			//level0的score计分原则是用level0的sst文件总数除以触发level0 compaction的数。如果level0的文件数达到触发compaction阈值的数，score的分数就会大于等于一
			score = float64(len(v.files[0])) / float64(internal.L0_CompactionTrigger)
		} else {
			//level n的score计分原则是用level n(n > 0)的所有sst文件大小总和除以一个文件大小常量（每层的文件大小常量的值不同）,如果超出那个常量值了，score的分数值就会大于1
			score = float64(totalFileSize(v.files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore { //如果最后统计完的score值大于1
			bestScore = score       //就不断替换score最大的那个值为bestScore
			compactionLevel = level //并且记录下level信息
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

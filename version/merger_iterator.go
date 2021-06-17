package version

import (
	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/sstable"
	"log"
)

type MergingIterator struct {
	list    []*sstable.Iterator
	current *sstable.Iterator
}

func NewMergingIterator(list []*sstable.Iterator) *MergingIterator {
	var iter MergingIterator
	iter.list = list
	return &iter
}

// Returns true iff the iterator is positioned at a valid node.
func (it *MergingIterator) Valid() bool {
	// 当前sstable不为空；当前sstable对应的dataIter不为空且dataIter的index有效
	// it.current.dataIter.index ∈ [0, len(it.current.dataIter.block.items))
	return it.current != nil && it.current.Valid()
}

func (it *MergingIterator) InternalKey() *internal.InternalKey {
	// 当前sstable正在访问的记录
	// it.current.dataIter.block.items[it.index]
	return it.current.InternalKey()
}

// 每次从多个sstable中获取最小值，然后同步推进，就是归并排序的思想
// Advances to the next position.
// REQUIRES: Valid()
func (it *MergingIterator) Next() {
	// 因为当前值已经读取过，所以行记录后移
	if it.current != nil {
		it.current.Next()
	}
	// 从多个sstable中获取最小值
	it.findSmallest()
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *MergingIterator) SeekToFirst() {
	for i := 0; i < len(it.list); i++ {
		it.list[i].SeekToFirst()
	}
	it.findSmallest()
}

func (it *MergingIterator) findSmallest() {
	log.Printf("findSmallest begin")
	defer log.Printf("findSmallest end")

	var smallest *sstable.Iterator = nil
	var small int
	for i := 0; i < len(it.list); i++ {
		if it.list[i].Valid() {
			// it.list[i].dataIter != nil
			if smallest == nil {
				smallest = it.list[i]
				small = i
			} else if internal.InternalKeyComparator(smallest.InternalKey(), it.list[i].InternalKey()) > 0 {
				smallest = it.list[i]
				small = i
			}
			log.Printf(" list %d : user key is %s \n", i, it.list[i].InternalKey().UserKey)
		}
	}
	if smallest != nil {
		log.Printf("smallest is %d : user key is %s \n", small, smallest.InternalKey().UserKey)
	}
	it.current = smallest
}

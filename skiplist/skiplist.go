package skiplist

import (
	"fmt"
	"github.com/merlin82/leveldb/internal"
	"math/rand"
	"sync"

	"github.com/merlin82/leveldb/utils"
)

const (
	kMaxHeight = 12
	kBranching = 4
)

type SkipList struct {
	maxHeight  int              //跳表最大高度
	head       *Node            //起始节点
	comparator utils.Comparator //比较
	mu         sync.RWMutex     //互斥锁
}

func New(comp utils.Comparator) *SkipList {
	var skiplist SkipList
	skiplist.head = newNode(nil, kMaxHeight)
	skiplist.maxHeight = 1     //当前的最大高度为1
	skiplist.comparator = comp //比较器
	return &skiplist
}

// 时间复杂度 O(log n)
// 类似单链表的添加节点操作，但是跳表需要考虑多个前驱和后继
// 不需要实现删除，因为sstable会merge
func (list *SkipList) Insert(key interface{}) {
	list.mu.Lock()
	defer list.mu.Unlock()

	_, prev := list.findGreaterOrEqual(key)
	//跳表是通过随机函数来决定新插入的数据除了放置在数据层的链表中，还放置到哪些索引层中。
	//比如随机函数生成了值 K，那我们就将这个结点添加到第一级到第 K 级这 K 级索引中。
	height := list.randomHeight()
	if height > list.maxHeight {
		for i := list.maxHeight; i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}
	x := newNode(key, height)
	for i := 0; i < height; i++ {
		x.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, x)
	}
}

// 时间复杂度 O(log n)
func (list *SkipList) Contains(key interface{}) bool {
	list.mu.RLock()
	defer list.mu.RUnlock()
	x, _ := list.findGreaterOrEqual(key)
	if x != nil && list.comparator(x.key, key) == 0 {
		return true
	}
	return false
}

func (list *SkipList) NewIterator() *Iterator {
	var it Iterator
	it.list = list
	return &it
}

func (list *SkipList) randomHeight() int {
	height := 1
	// 25% 的概率会变成父节点
	for height < kMaxHeight && (rand.Intn(kBranching) == 0) {
		height++
	}
	return height
}

// 记录大于等于key的前驱，如果是单链表则为一个node即可，但是跳表是多层需要记录level和node的关系
func (list *SkipList) findGreaterOrEqual(key interface{}) (*Node, [kMaxHeight]*Node) {
	var prev [kMaxHeight]*Node // 定长Node类型数组
	x := list.head
	level := list.maxHeight - 1 // 高度是3， level最高层是2
	for true {
		next := x.getNext(level)
		if list.keyIsAfterNode(key, next) {
			x = next //如果keyIsAfterNode返回的值为-1，就代表得到了期望的结果。
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				// Switch to next list
				level--
			}
		}
	}
	return nil, prev
}

func (list *SkipList) findLessThan(key interface{}) *Node {
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil || list.comparator(next.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}
func (list *SkipList) findlast() *Node {
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}

func (list *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (list.comparator(n.key, key) < 0) //小于0就说明比较的结果需要为-1，
}

func (list *SkipList) Print() string {
	ss := ""
	for level := 0; level < kMaxHeight; level++ {
		sss := ""
		x := list.head.getNext(level)
		for x != nil {
			sss += fmt.Sprintf("%s-v%d ", x.key.(*internal.InternalKey).UserKey, x.key.(*internal.InternalKey).Seq)
			x = x.getNext(level)
		}
		ss = fmt.Sprintf("[level = %02d] %s\n", level, sss) + ss
	}
	return ss
}

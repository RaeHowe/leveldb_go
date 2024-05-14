package internal

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

type ValueType int8

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

type InternalKey struct {
	UserKey   []byte    // user key
	Seq       uint64    // seq num
	Type      ValueType // operator type
	UserValue []byte
}

// NewInternalKey 通过seq num, type, user key, user value构造出来internal key
func NewInternalKey(seq uint64, valueType ValueType, key, value []byte) *InternalKey {
	var internalKey InternalKey
	internalKey.Seq = seq
	internalKey.Type = valueType

	internalKey.UserKey = make([]byte, len(key))
	copy(internalKey.UserKey, key) //rocksdb官方代码这里也是一个内存地址拷贝的过程
	internalKey.UserValue = make([]byte, len(value))
	copy(internalKey.UserValue, value)

	return &internalKey
}

func (key *InternalKey) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, key.Seq)
	binary.Write(w, binary.LittleEndian, key.Type)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserKey)))
	binary.Write(w, binary.LittleEndian, key.UserKey)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserValue)))
	return binary.Write(w, binary.LittleEndian, key.UserValue)
}

func (key *InternalKey) DecodeFrom(r io.Reader) error {
	var tmp int32
	binary.Read(r, binary.LittleEndian, &key.Seq)
	binary.Read(r, binary.LittleEndian, &key.Type)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserKey = make([]byte, tmp)
	binary.Read(r, binary.LittleEndian, key.UserKey)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserValue = make([]byte, tmp)
	return binary.Read(r, binary.LittleEndian, key.UserValue)
}

func LookupKey(key []byte) *InternalKey {
	//下面这里的seq num搞了个最大值，默认是要取数据的最新值
	return NewInternalKey(math.MaxUint64, TypeValue, key, nil) // seq num, type, user key, user value
}

/*
下面的比较函数，就类似rocksdb里面对key的比较器
inline int InternalKeyComparator::Compare(const Slice& akey,

											  const Slice& bkey) const {
	  // Order by:
	  //    increasing user key (according to user-supplied comparator)
	  //    decreasing sequence number
	  //    decreasing type (though sequence# should be enough to disambiguate)
	  int r = user_comparator_.Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
	  if (r == 0) {
		const uint64_t anum =
			DecodeFixed64(akey.data() + akey.size() - kNumInternalBytes);
		const uint64_t bnum =
			DecodeFixed64(bkey.data() + bkey.size() - kNumInternalBytes);
		if (anum > bnum) {
		  r = -1;
		} else if (anum < bnum) {
		  r = +1;
		}
	  }
	  return r;
	}
*/
// internalkey的比较器
func InternalKeyComparator(a, b interface{}) int {
	// Order by:
	//    increasing user key (according to user-supplied comparator)
	//    decreasing sequence number
	//    decreasing type (though sequence# should be enough to disambiguate)
	/*
		internalKey(user key + seq num + operator type)
		先根据user key排序；如果user key相同，则根据seq排序；seq相同根据操作类型排序
	*/
	aKey := a.(*InternalKey)
	bKey := b.(*InternalKey)
	r := UserKeyComparator(aKey.UserKey, bKey.UserKey) //比较user key，如果a的user key小于b的话，那么返回-1（保持a在b之前）
	if r == 0 {
		//到这个逻辑说明a和b的user key是相同的
		anum := aKey.Seq
		bnum := bKey.Seq
		if anum > bnum {
			//这里和user key的返回是相反的，因为rocksdb里面，会把相同key，但是更新的数据排在前面，所以如果a的seq大于b的话，就返回-1（保持a在b之前）
			r = -1
		} else if anum < bnum {
			//如果a的seq小于b的seq的话，就需要交换a和b的位置
			r = +1
		}
	}

	//todo:待补充对type的判断

	return r
}

func UserKeyComparator(a, b interface{}) int {
	aKey := a.([]byte)
	bKey := b.([]byte)
	return bytes.Compare(aKey, bKey)
}

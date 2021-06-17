package main

import (
	"github.com/merlin82/leveldb"
	"math/rand"
	"strconv"
)

func makeKeyValue() (string, string) {
	intn := rand.Intn(10)
	return strconv.Itoa(intn), strconv.Itoa(intn)
}

func main() {
	db := leveldb.Open("./test/a")
	for i := 0; i < 100; i++ {
		key, val := makeKeyValue()
		_ = db.Put([]byte(key), []byte(val))
	}
	db.PrintMem()
	db.PrintVersion()
	_, _ = db.Get([]byte("cccc"))
	_ = db.Delete([]byte("cccc"))
}

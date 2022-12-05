package tree

import (
	"github.com/ipld/go-ipld-prime"
	"sync"
)

type pair struct {
	key   []byte
	value ipld.Node
}

func NewSearchIterator() *SearchIterator {
	return &SearchIterator{
		result: make([]pair, 0),
	}
}

type SearchIterator struct {
	idx    int
	mtx    sync.Mutex
	result []pair
}

func (si *SearchIterator) ReceivePair(key []byte, value ipld.Node) {
	si.result = append(si.result, pair{
		key:   key,
		value: value,
	})
}

func (si *SearchIterator) Next() ([]byte, ipld.Node) {
	// avoid concurrent Next() to read
	si.mtx.Lock()
	defer si.mtx.Unlock()
	res := si.result[si.idx]
	si.idx++
	return res.key, res.value
}

func (si *SearchIterator) IsEmpty() bool {
	return si.idx == 0
}

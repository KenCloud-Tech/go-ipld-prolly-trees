package tree

import (
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"io"
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

func (si *SearchIterator) receivePair(key []byte, value ipld.Node) {
	si.result = append(si.result, pair{
		key:   key,
		value: value,
	})
}

func (si *SearchIterator) Next() (ipld.Node, ipld.Node, error) {
	k, v, err := si.NextPair()
	return basicnode.NewBytes(k), v, err
}

func (si *SearchIterator) NextPair() ([]byte, ipld.Node, error) {
	// avoid concurrent Next() to read
	si.mtx.Lock()
	defer si.mtx.Unlock()
	if si.Done() {
		return nil, nil, io.EOF
	}
	res := si.result[si.idx]
	si.idx++
	return res.key, res.value, nil
}

func (si *SearchIterator) Done() bool {
	return si.idx == len(si.result)
}

func (si *SearchIterator) IsEmpty() bool {
	return len(si.result) == 0
}

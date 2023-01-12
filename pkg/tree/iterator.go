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

func NewIterator() *Iterator {
	return &Iterator{
		result: make([]pair, 0),
	}
}

type Iterator struct {
	idx    int
	mtx    sync.Mutex
	result []pair
}

func (si *Iterator) receivePair(key []byte, value ipld.Node) {
	si.mtx.Lock()
	defer si.mtx.Unlock()
	si.result = append(si.result, pair{
		key:   key,
		value: value,
	})
}

func (si *Iterator) Next() (ipld.Node, ipld.Node, error) {
	k, v, err := si.NextPair()
	return basicnode.NewBytes(k), v, err
}

func (si *Iterator) NextPair() ([]byte, ipld.Node, error) {
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

func (si *Iterator) Done() bool {
	return si.idx == len(si.result)
}

func (si *Iterator) IsEmpty() bool {
	return len(si.result) == 0
}

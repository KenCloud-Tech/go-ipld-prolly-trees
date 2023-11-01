package tree

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"io"
	"sync"
)

const DefaultChannelSize = 20

type pair struct {
	key   []byte
	value ipld.Node
}

func NewIterator(size int) *Iterator {
	if size == -1 {
		size = DefaultChannelSize
	} else if size < 0 {
		panic(fmt.Sprintf("invalid result channel size: %d", size))
	}
	return &Iterator{
		result: make(chan pair, size),
	}
}

type Iterator struct {
	mtx    sync.Mutex
	result chan pair
	done   bool
}

func (si *Iterator) receivePair(key []byte, value ipld.Node) {
	if si.done {
		panic("channel has closed")
	}
	si.result <- pair{
		key:   key,
		value: value,
	}
}

func (si *Iterator) finish() {
	if si.done == true {
		panic("repeated closing")
	}
	si.done = true
	close(si.result)
}

func (si *Iterator) Next() (ipld.Node, ipld.Node, error) {
	k, v, err := si.NextPair()
	return basicnode.NewString(string(k)), v, err
}

func (si *Iterator) NextPair() ([]byte, ipld.Node, error) {
	if si.Done() {
		return nil, nil, io.EOF
	}
	res := <-si.result
	return res.key, res.value, nil
}

func (si *Iterator) Done() bool {
	return si.done && len(si.result) == 0
}

func (si *Iterator) IsEmpty() bool {
	return len(si.result) == 0
}

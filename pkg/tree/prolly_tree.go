package tree

import (
	"bytes"
	"errors"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	. "go-ipld-prolly-trees/pkg/tree/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

var (
	KeyNotFound = errors.New("key not found")
)

type ProllyTree struct {
	rootCid    cid.Cid
	root       *ProllyNode
	ns         types.NodeStore
	treeConfig *ChunkConfig
}

func (pt *ProllyTree) Get(key []byte) (ipld.Node, error) {
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}
	if !cur.IsValid() || DefaultCompareFunc(cur.GetKey(), key) != 0 {
		return nil, KeyNotFound
	}

	return cur.GetValue(), nil
}

func (pt *ProllyTree) Search(prefix []byte) (*SearchIterator, error) {
	cur, err := CursorAtItem(pt.root, prefix, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}
	iter := NewSearchIterator()
	for {
		if !cur.IsValid() {
			break
		}
		key := cur.GetKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		val := cur.GetValue()

		iter.ReceivePair(key, val)

		err = cur.AdvanceCursor()
		if err != nil {
			return nil, err
		}
	}

	if iter.IsEmpty() {
		return nil, KeyNotFound
	}

	return iter, nil
}

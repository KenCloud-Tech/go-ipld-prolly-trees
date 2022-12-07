package tree

import (
	"bytes"
	"context"
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

func LoadProllyTreeFromRootNode(rootNode *ProllyRoot, ns types.NodeStore) (*ProllyTree, error) {
	prollyRootNode, err := ns.ReadNode(context.Background(), rootNode.RootCid)
	if err != nil {
		return nil, err
	}

	return &ProllyTree{
		rootCid:    rootNode.RootCid,
		root:       prollyRootNode,
		ns:         ns,
		treeConfig: &rootNode.Config,
	}, nil
}

func LoadProllyTreeFromRootCid(rootCid cid.Cid, ns types.NodeStore) (*ProllyTree, error) {
	rootNode, err := ns.ReadRoot(context.Background(), rootCid)
	if err != nil {
		return nil, err
	}
	return LoadProllyTreeFromRootNode(rootNode, ns)
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

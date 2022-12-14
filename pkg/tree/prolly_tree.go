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
	treeCid    cid.Cid
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
	config, err := ns.ReadTreeConfig(context.Background(), rootNode.ConfigCid)
	if err != nil {
		return nil, err
	}

	return &ProllyTree{
		rootCid:    rootNode.RootCid,
		root:       prollyRootNode,
		ns:         ns,
		treeConfig: config,
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

		err = cur.Advance()
		if err != nil {
			return nil, err
		}
	}

	if iter.IsEmpty() {
		return nil, KeyNotFound
	}

	return iter, nil
}

func (pt *ProllyTree) Put(ctx context.Context, key []byte, val ipld.Node) error {
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return err
	}
	framework, err := NewFramework(ctx, pt.ns, pt.treeConfig, cur)
	if err != nil {
		return err
	}
	if cur.IsValid() {
		// modify
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			err := framework.Append(ctx, key, val)
			if err != nil {
				return err
			}
			err = cur.Advance()
			if err != nil {
				return err
			}
		} else {
			//add new pair
			err := framework.Append(ctx, key, val)
			if err != nil {
				return err
			}
		}
	}
	newTree, err := framework.BuildTree(ctx)
	if err != nil {
		return err
	}
	pt.root = newTree.root
	pt.rootCid = newTree.rootCid
	pt.treeCid = newTree.treeCid
	return nil
}

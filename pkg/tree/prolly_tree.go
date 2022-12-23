package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	. "go-ipld-prolly-trees/pkg/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
	"io"
)

var (
	KeyNotFound = errors.New("key not found")
)

type ProllyTree struct {
	treeCid    cid.Cid
	rootCid    cid.Cid
	root       *ProllyNode
	ns         types.NodeStore
	treeConfig *TreeConfig

	mutating  bool
	mutations *Mutations
}

func LoadProllyTreeFromRootNode(rootNode *ProllyTreeNode, ns types.NodeStore) (*ProllyTree, error) {
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
	if pt.mutating {
		cacheNode, err := pt.mutations.Get(key)
		if err != nil {
			return nil, err
		}
		if cacheNode != nil {
			return cacheNode, nil
		}
	}

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

func (pt *ProllyTree) Mutate() error {
	pt.mutations = NewMutations()
	pt.mutating = true
	return nil
}

func (pt *ProllyTree) Put(ctx context.Context, key []byte, val ipld.Node) error {
	if !pt.mutating {
		return fmt.Errorf("please call ProllyTree.Mutate firstly")
	}
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return err
	}

	mut := &Mutation{
		key: key,
		val: val,
	}

	if cur.IsValid() {
		// modify
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			mut.op = modify
		} else {
			//add new pair
			mut.op = add
		}
		err = pt.mutations.addMutation(mut)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pt *ProllyTree) Delete(ctx context.Context, key []byte) error {
	if !pt.mutating {
		return fmt.Errorf("please call ProllyTree.Mutate firstly")
	}
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return err
	}

	if cur.IsValid() {
		// delete
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			err = pt.mutations.addMutation(&Mutation{
				key: key,
				op:  remove,
			})
			if err != nil {
				return err
			}
		}
		// if not exist, ignore
	}

	return nil
}

func (pt *ProllyTree) Rebuild(ctx context.Context) error {
	mut, err := pt.mutations.NextMutation()
	if err != nil {
		return err
	}
	cur, err := CursorAtItem(pt.root, mut.key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return err
	}
	framework, err := NewFramework(ctx, pt.ns, pt.treeConfig, cur)
	if err != nil {
		return err
	}
	for {
		if cur.IsValid() {
			// modify
			if DefaultCompareFunc(cur.GetKey(), mut.key) == 0 {
				if mut.op == modify {
					err := framework.Append(ctx, mut.key, mut.val)
					if err != nil {
						return err
					}
					err = framework.AdvanceCursor(ctx)
					if err != nil {
						return err
					}
				} else if mut.op == remove {
					err = framework.AdvanceCursor(ctx)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("invalid mutation: %#v", mut)
				}
			} else {
				//add new pair
				if mut.op == add {
					err := framework.Append(ctx, mut.key, mut.val)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("invalid mutation: %#v", mut)
				}
			}
			mut, err = pt.mutations.NextMutation()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			cur, err = CursorAtItem(pt.root, mut.key, DefaultCompareFunc, pt.ns)
			if err != nil {
				return err
			}
			// todo move cursor to next mutation location and append items between them
			err = framework.appendToCursor(ctx, cur)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("invalid cursor")
		}
	}

	newTree, err := framework.BuildTree(ctx)
	if err != nil {
		return err
	}

	pt.root = newTree.root
	pt.rootCid = newTree.rootCid
	pt.treeCid = newTree.treeCid

	pt.mutating = false
	pt.mutations = nil

	return nil
}

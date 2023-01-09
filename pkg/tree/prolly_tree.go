package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"io"
)

var (
	KeyNotFound = errors.New("Key not found")
)

type ProllyTree struct {
	ProllyTreeNode
	root       *ProllyNode
	Ns         NodeStore
	treeConfig *TreeConfig

	mutating  bool
	mutations *Mutations
}

func (pt *ProllyTree) loadProllyTreeFromRootNode(ns NodeStore) error {
	//if pt.root == nil {
	prollyRootNode, err := ns.ReadNode(context.Background(), pt.RootCid)
	if err != nil {
		return err
	}
	pt.root = prollyRootNode
	//}

	//if pt.treeConfig == nil {
	config, err := ns.ReadTreeConfig(context.Background(), pt.ConfigCid)
	if err != nil {
		return err
	}
	pt.treeConfig = config
	//}

	pt.Ns = ns
	return nil
}

func LoadProllyTreeFromRootCid(rootCid cid.Cid, ns NodeStore) (*ProllyTree, error) {
	tree, err := ns.ReadTree(context.Background(), rootCid)
	if err != nil {
		return nil, err
	}
	err = tree.loadProllyTreeFromRootNode(ns)
	return tree, err
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

	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.Ns)
	if err != nil {
		return nil, err
	}
	if !cur.IsValid() || DefaultCompareFunc(cur.GetKey(), key) != 0 {
		return nil, KeyNotFound
	}

	return cur.GetValue(), nil
}

func (pt *ProllyTree) Search(prefix []byte) (*SearchIterator, error) {
	cur, err := CursorAtItem(pt.root, prefix, DefaultCompareFunc, pt.Ns)
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
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.Ns)
	if err != nil {
		return err
	}

	mut := &Mutation{
		Key: key,
		Val: val,
	}

	if cur.IsValid() {
		// Modify
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			mut.Op = Modify
		} else {
			//Add new pair
			mut.Op = Add
		}
		err = pt.mutations.AddMutation(mut)
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
	cur, err := CursorAtItem(pt.root, key, DefaultCompareFunc, pt.Ns)
	if err != nil {
		return err
	}

	if cur.IsValid() {
		// delete
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			err = pt.mutations.AddMutation(&Mutation{
				Key: key,
				Op:  Remove,
			})
			if err != nil {
				return err
			}
		}
		// if not exist, ignore
	}

	return nil
}

func (pt *ProllyTree) Rebuild(ctx context.Context) (cid.Cid, error) {
	// avoid wrong adding mutation while rebuilding
	pt.mutations.Finish()

	mut, err := pt.mutations.NextMutation()
	if err != nil {
		return cid.Undef, err
	}
	cur, err := CursorAtItem(pt.root, mut.Key, DefaultCompareFunc, pt.Ns)
	if err != nil {
		return cid.Undef, err
	}
	framework, err := NewFramework(ctx, pt.Ns, pt.treeConfig, cur)
	if err != nil {
		return cid.Undef, err
	}
	for {
		if cur.IsValid() {
			// Modify
			if DefaultCompareFunc(cur.GetKey(), mut.Key) == 0 {
				if mut.Op == Modify {
					err := framework.Append(ctx, mut.Key, mut.Val)
					if err != nil {
						return cid.Undef, err
					}
					err = framework.AdvanceCursor(ctx)
					if err != nil {
						return cid.Undef, err
					}
				} else if mut.Op == Remove {
					err = framework.AdvanceCursor(ctx)
					if err != nil {
						return cid.Undef, err
					}
				} else {
					return cid.Undef, fmt.Errorf("invalid mutation: %#v", mut)
				}
			} else {
				//Add new pair
				if mut.Op == Add {
					err := framework.Append(ctx, mut.Key, mut.Val)
					if err != nil {
						return cid.Undef, err
					}
				} else {
					return cid.Undef, fmt.Errorf("invalid mutation: %#v", mut)
				}
			}
			mut, err = pt.mutations.NextMutation()
			if err == io.EOF {
				break
			} else if err != nil {
				return cid.Undef, err
			}

			cur, err = CursorAtItem(pt.root, mut.Key, DefaultCompareFunc, pt.Ns)
			if err != nil {
				return cid.Undef, err
			}
			// todo move cursor to next mutation location and append items between them
			err = framework.appendToCursor(ctx, cur)
			if err != nil {
				return cid.Undef, err
			}
		} else {
			return cid.Undef, fmt.Errorf("invalid cursor")
		}
	}

	newTree, newTreeCid, err := framework.BuildTree(ctx)
	if err != nil {
		return cid.Undef, err
	}

	pt.ProllyTreeNode = newTree.ProllyTreeNode
	pt.root = newTree.root
	pt.treeConfig = newTree.treeConfig
	pt.Ns = newTree.Ns

	pt.mutating = false
	pt.mutations = nil

	return newTreeCid, nil
}

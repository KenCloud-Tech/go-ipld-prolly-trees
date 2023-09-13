package tree

import (
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
	ProllyRoot
	root       ProllyNode
	ns         NodeStore
	treeConfig TreeConfig

	mutating  bool
	mutations *Mutations
}

// Segments in proving a key-value pair is in a tree
type ProofSegment struct {
	// Which node in the tree to perform the lookup on
	Node cid.Cid
	// Which index the key is in
	Index int
}

type Proof []ProofSegment

func (pt *ProllyTree) LoadProllyTreeFromRootNode(ns NodeStore) error {
	prollyRootNode, err := ns.ReadNode(context.Background(), pt.Root)
	if err != nil {
		return err
	}
	pt.root = *prollyRootNode

	config, err := ns.ReadTreeConfig(context.Background(), pt.Config)
	if err != nil {
		return err
	}
	pt.treeConfig = *config

	pt.ns = ns
	return nil
}

func LoadProllyTreeFromRootCid(rootCid cid.Cid, ns NodeStore) (*ProllyTree, error) {
	tree, err := ns.ReadTree(context.Background(), rootCid)
	if err != nil {
		return nil, err
	}
	err = tree.LoadProllyTreeFromRootNode(ns)
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

	cur, err := CursorAtItem(&pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}
	if !cur.IsValid() || DefaultCompareFunc(cur.GetKey(), key) != 0 {
		return nil, KeyNotFound
	}

	return cur.GetValue(), nil
}

func (pt *ProllyTree) GetProof(key []byte) (Proof, error) {
	if pt.mutating {
		return nil, fmt.Errorf("Cannot get proof while tree is being mutated. Apply changes with Rebuild first.")
	}

	cur, err := CursorAtItem(&pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}
	if !cur.IsValid() || DefaultCompareFunc(cur.GetKey(), key) != 0 {
		return nil, KeyNotFound
	}

	proof := Proof{}

	if cur.node.IsLeaf {
		cur = cur.parent
	}

	for cur != nil {
		link := cur.GetLink()
		index := cur.GetIndex()
		proof = append(proof, ProofSegment{
			Node:  link,
			Index: index,
		})
		cur = cur.parent
	}

	return proof, nil
}

func (pt *ProllyTree) Search(ctx context.Context, start []byte, end []byte) (*Iterator, error) {
	if start == nil && end == nil {
		return nil, fmt.Errorf("empty start and end key")
	}
	var err error
	if start == nil {
		start, err = pt.FirstKey()
		if err != nil {
			return nil, err
		}
	}
	if end == nil {
		end, err = pt.LastKey()
		if err != nil {
			return nil, err
		}
	}

	cur, err := CursorAtItem(&pt.root, start, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}
	iter := NewIterator(-1)
	go func() {
		defer iter.finish()
		for {
			select {
			case <-ctx.Done():
				// todo: return error info
				return
			default:
			}
			if !cur.IsValid() {
				break
			}
			key := cur.GetKey()

			if DefaultCompareFunc(key, start) < 0 || DefaultCompareFunc(key, end) > 0 {
				break
			}

			val := cur.GetValue()

			iter.receivePair(key, val)

			err = cur.Advance()
			if err != nil {
				// todo: return error info
				return
			}
		}
	}()

	return iter, nil
}

func (pt *ProllyTree) Mutate() error {
	pt.mutations = NewMutations()
	pt.mutating = true
	return nil
}

func (pt *ProllyTree) IsMutating() bool {
	return pt.mutating
}

func (pt *ProllyTree) Put(ctx context.Context, key []byte, val ipld.Node) error {
	if !pt.mutating {
		return fmt.Errorf("please call ProllyTree.Mutate firstly")
	}
	cur, err := CursorAtItem(&pt.root, key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return err
	}

	mut := &Mutation{
		Key: key,
		Val: val,
	}

	if !cur.IsValid() && !cur.node.IsEmpty() {
		panic("invalid cursor")
	}

	// empty tree
	if cur.node.IsEmpty() {
		//Add new pair
		mut.Op = Add
	} else {
		// Modify
		if DefaultCompareFunc(cur.GetKey(), key) == 0 {
			mut.Op = Modify
		} else {
			//Add new pair
			mut.Op = Add
		}
	}

	err = pt.mutations.AddMutation(mut)
	if err != nil {
		return err
	}

	return nil
}

func (pt *ProllyTree) Delete(ctx context.Context, key []byte) error {
	if !pt.mutating {
		return fmt.Errorf("please call ProllyTree.Mutate firstly")
	}
	cur, err := CursorAtItem(&pt.root, key, DefaultCompareFunc, pt.ns)
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
	cur, err := CursorAtItem(&pt.root, mut.Key, DefaultCompareFunc, pt.ns)
	if err != nil {
		return cid.Undef, err
	}
	framework, err := NewFramework(ctx, pt.ns, &pt.treeConfig, cur)
	if err != nil {
		return cid.Undef, err
	}
	for {

		if mut.Op == Add {
			if !cur.node.IsEmpty() && cur.IsValid() && DefaultCompareFunc(cur.GetKey(), mut.Key) == 0 {
				return cid.Undef, fmt.Errorf("can not add exist key in the tree")
			}
			err := framework.Append(ctx, mut.Key, mut.Val)
			if err != nil {
				return cid.Undef, err
			}
		} else {
			if DefaultCompareFunc(cur.GetKey(), mut.Key) != 0 {
				return cid.Undef, fmt.Errorf("modified or remove key should be the same with origin key")
			}
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
		}

		mut, err = pt.mutations.NextMutation()
		if err == io.EOF {
			break
		} else if err != nil {
			return cid.Undef, err
		}

		cur, err = CursorAtItem(&pt.root, mut.Key, DefaultCompareFunc, pt.ns)
		if err != nil {
			return cid.Undef, err
		}
		// the key is bigger than all keys in the tree, advance it
		if cur.IsBiggerThanTheNode(mut.Key) {
			err = cur.Advance()
			if err != nil {
				return cid.Undef, err
			}
		}

		err = framework.appendToCursor(ctx, cur)
		if err != nil {
			return cid.Undef, err
		}

	}

	newTree, newTreeCid, err := framework.BuildTree(ctx)
	if err != nil {
		return cid.Undef, err
	}

	pt.ProllyRoot = newTree.ProllyRoot
	pt.root = newTree.root
	pt.treeConfig = newTree.treeConfig
	pt.ns = newTree.ns

	pt.mutating = false
	pt.mutations = nil

	return newTreeCid, nil
}

func (pt *ProllyTree) NodeStore() NodeStore {
	return pt.ns
}

func (pt *ProllyTree) TreeConfig() TreeConfig {
	return pt.treeConfig
}

func (pt *ProllyTree) TreeCount() uint32 {
	return pt.root.totalPairCount()
}

// get the first(smallest) key of the tree
func (pt *ProllyTree) FirstKey() ([]byte, error) {
	n := &pt.root
	var err error
	for !n.IsLeaf {
		n, err = pt.ns.ReadNode(context.Background(), n.GetIdxLink(0))
		if err != nil {
			return nil, err
		}
	}
	return n.GetIdxKey(0), nil
}

// get the last(largest) key of the tree
func (pt *ProllyTree) LastKey() ([]byte, error) {
	return pt.root.GetIdxKey(pt.root.ItemCount() - 1), nil
}

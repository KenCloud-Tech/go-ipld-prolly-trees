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
	ProllyRoot
	root       ProllyNode
	ns         NodeStore
	treeConfig TreeConfig
	treeCid    *cid.Cid

	mutating  bool
	mutations *Mutations
}

// Segments in proving a key-value pair is in a tree
type ProofSegment struct {
	// Which TreeNode this key is in
	// For last segment this should be the root CID
	Node cid.Cid
	// Which index in the node the key is in
	// For leaf nodes this is the value
	// For non-leafs this is where the prev node was linked
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

	// Only prove leaves
	if !cur.node.IsLeaf {
		return nil, KeyNotFound
	}

	for cur.parent != nil {
		index := cur.GetIndex()
		link := cur.parent.GetLink()

		proof = append(proof, ProofSegment{
			Node:  link,
			Index: index,
		})
		cur = cur.parent
	}

	// Add top level tree node
	index := cur.GetIndex()
	proof = append(proof, ProofSegment{
		// Get root tree node cid
		Node:  pt.Root,
		Index: index,
	})

	// Add prolly root
	proof = append(proof, ProofSegment{
		// Get prolly tree cid as final step
		Node: *pt.treeCid,
		// Index is 2 cause of cbor encoding
		Index: 2,
	})

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
	pt.treeCid = newTree.treeCid

	pt.mutating = false
	pt.mutations = nil

	return newTreeCid, nil
}

func (pt *ProllyTree) Diff(other *ProllyTree) (*Diffs, error) {
	diffs := NewDiffs()
	otherConfig := other.TreeConfig()
	config := pt.TreeConfig()
	if !config.Equal(&otherConfig) {
		return nil, fmt.Errorf("diff between trees with different config is not allowed")
	}
	if pt.Root.Equals(other.Root) {
		return nil, nil
	}
	firstKeyBase, err := pt.FirstKey()
	if err != nil {
		return nil, err
	}
	curBase, err := CursorAtItem(&pt.root, firstKeyBase, DefaultCompareFunc, pt.ns)
	if err != nil {
		return nil, err
	}

	firstKeyOther, err := pt.FirstKey()
	if err != nil {
		return nil, err
	}
	curOther, err := CursorAtItem(&other.root, firstKeyOther, DefaultCompareFunc, other.ns)
	if err != nil {
		return nil, err
	}

	go func() {
		defer diffs.Close()
		for {
			if !curBase.IsValid() {
				if !curOther.IsValid() {
					return
				} else {
					for curOther.IsValid() {
						err = diffs.AddMutation(&Mutation{
							Key: curOther.GetKey(),
							Val: curOther.GetValue(),
							Op:  Add,
						})
						if err != nil {
							panic(err)
						}
						err = curOther.Advance()
						if err != nil {
							panic(err)
						}
					}
				}
			}
			// if new tree cursor arrived end firstly, return(ignore delete option)
			if !curOther.IsValid() {
				return
			}

			cmp := DefaultCompareFunc(curBase.GetKey(), curOther.GetKey())
			// ignore keys in the base which are missing from the new tree
			if cmp < 0 {
				err = curBase.Advance()
				if err != nil {
					// todo: better error handling
					panic(err)
				}
				continue
			} else if cmp > 0 {
				err = diffs.AddMutation(&Mutation{
					Key: curOther.GetKey(),
					Val: curOther.GetValue(),
					Op:  Add,
				})
				if err != nil {
					panic(err)
				}
			} else {
				// if k/v pair equal, try skipping common parts
				if bytes.Equal(EncodeNode(curBase.GetValue()), EncodeNode(curOther.GetValue())) {
					err = curBase.SkipCommon(curOther)
					if err != nil {
						panic(err)
					}
				} else {
					err = diffs.AddMutation(&Mutation{
						Key: curOther.GetKey(),
						Val: curOther.GetValue(),
						Op:  Modify,
					})
					if err != nil {
						panic(err)
					}
				}
			}
			err = curBase.Advance()
			if err != nil {
				panic(err)
			}
			err = curOther.Advance()
			if err != nil {
				panic(err)
			}
		}
	}()

	return diffs, nil
}

func (pt *ProllyTree) Merge(ctx context.Context, other *ProllyTree) error {
	diffs, err := pt.Diff(other)
	if err != nil {
		return err
	}
	err = pt.Mutate()
	if err != nil {
		return err
	}
	for {
		mut, err := diffs.NextMutations()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if mut.Op == Add || mut.Op == Modify {
			err = pt.Put(ctx, mut.Key, mut.Val)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unsupported action now")
		}
	}
	_, err = pt.Rebuild(ctx)
	return err
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

func (pt *ProllyTree) TreeCid() (*cid.Cid, error) {
	if pt.mutating {
		return nil, fmt.Errorf("Cannot get tree cid while tree is being mutated. Apply changes with Rebuild first.")
	}
	return pt.treeCid, nil
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

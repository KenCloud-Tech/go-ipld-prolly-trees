package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	. "go-ipld-prolly-trees/pkg/tree/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

type nodeBuilder struct {
	keys      [][]byte
	values    []ipld.Node
	links     []cid.Cid
	level     int
	configCid cid.Cid
}

func (nb *nodeBuilder) count() int {
	return len(nb.keys)
}

func (nb *nodeBuilder) addLeafPair(key []byte, val ipld.Node) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, val)
}

func (nb *nodeBuilder) addBranchPair(key, val []byte) {
	nb.keys = append(nb.keys, key)
	_, c, err := cid.CidFromBytes(val)
	if err != nil {
		panic(err)
	}
	nb.links = append(nb.links, c)
}

func (nb *nodeBuilder) clean() {
	nb.keys = nil
	nb.values = nil
	nb.links = nil
}

func (nb *nodeBuilder) build() *ProllyNode {
	n := &ProllyNode{
		Keys:   nb.keys,
		Level:  nb.level,
		Config: nb.configCid,
	}
	if nb.level == 0 {
		n.Values = nb.values
	} else {
		n.Links = nb.links
	}
	nb.clean()
	return n
}

type LevelBuilder struct {
	config        *ChunkConfig
	level         int
	cursor        *Cursor
	nodeBuffer    *nodeBuilder
	parentBuilder *LevelBuilder
	nodeStore     types.NodeStore
	splitter      Splitter
	framework     *Framework
	done          bool
}

func newLevelBuilder(ctx context.Context, level int, ns types.NodeStore, config *ChunkConfig, configCid cid.Cid, cur *Cursor, frameWork *Framework) *LevelBuilder {
	if config == nil {
		panic("nil config")
	}
	if !configCid.Defined() {
		panic("nil configCid")
	}

	var splitter Splitter
	switch config.ChunkStrategy {
	case WeibullThreshold:
		splitter = &WeibullSplitter{
			config: config,
		}
	default:
		panic(fmt.Errorf("unsupported chunk strategy: %s", config.ChunkStrategy))
	}

	nb := &nodeBuilder{
		level:     level,
		configCid: configCid,
	}

	lb := &LevelBuilder{
		config:     config,
		level:      level,
		cursor:     cur,
		nodeBuffer: nb,
		nodeStore:  ns,
		splitter:   splitter,
		framework:  frameWork,
	}

	return lb
}

func (lb *LevelBuilder) append(ctx context.Context, key []byte, leafValue ipld.Node, link *cid.Cid) (bool, error) {
	if lb.done {
		return false, fmt.Errorf("append pair in done builder")
	}
	if lb.level == 0 && leafValue == nil || lb.level > 0 && !link.Defined() {
		panic("mismatched input type with level")
	}
	var valBytes []byte
	if lb.level == 0 {
		valBuffer := new(bytes.Buffer)
		// maybe configurable
		err := dagcbor.Encode(leafValue, valBuffer)
		if err != nil {
			return false, err
		}
		valBytes = valBuffer.Bytes()
		lb.nodeBuffer.addLeafPair(key, leafValue)
	} else if lb.level > 0 {
		valBytes = link.Bytes()
		lb.nodeBuffer.addBranchPair(key, valBytes)
	} else {
		panic("invalid level")
	}

	err := lb.splitter.Append(key, valBytes)
	if err != nil {
		return false, err
	}

	// boundary is true , but it's branch node with only one pair k/v, so we can not split here.
	// if split in the state, it will generate boundary infinitely(its parent node will generate in the same state too)
	if lb.splitter.IsBoundary() && !(lb.level != 0 && lb.nodeBuffer.count() == 1) {
		err = lb.splitBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func buildAndSaveNode(ctx context.Context, nb *nodeBuilder, prefix *cid.Prefix, ns types.NodeStore) (*ProllyNode, cid.Cid, error) {
	if !(nb.count() > 0) {
		return nil, cid.Undef, fmt.Errorf("invalid nodeBuilder to build")
	}
	node := nb.build()
	addr, err := ns.WriteNode(ctx, node, prefix)
	if err != nil {
		return nil, cid.Undef, err
	}
	return node, addr, nil
}

func (lb *LevelBuilder) splitBoundary(ctx context.Context) error {
	node, addr, err := buildAndSaveNode(ctx, lb.nodeBuffer, nil, lb.nodeStore)
	if err != nil {
		return err
	}

	key := node.GetIdxKey(0)
	firstKey := make([]byte, len(key))
	copy(firstKey, key)

	if lb.parentBuilder == nil {
		err = lb.createParentLevelBuilder(ctx)
		if err != nil {
			return err
		}
	}

	_, err = lb.parentBuilder.append(ctx, firstKey, nil, &addr)
	lb.splitter.Reset()

	return nil
}

func (lb *LevelBuilder) createParentLevelBuilder(ctx context.Context) error {
	if lb.parentBuilder != nil {
		panic("invalid action, parent level builder has already existed!")
	}

	var err error
	var parentCursor *Cursor

	if lb.cursor != nil && lb.cursor.parentCursor != nil {
		parentCursor = lb.cursor.parentCursor
	}

	lb.parentBuilder = newLevelBuilder(ctx, lb.level+1, lb.nodeStore, lb.config, lb.nodeBuffer.configCid, parentCursor, lb.framework)
	lb.framework.builders = append(lb.framework.builders, lb.parentBuilder)
	return err
}

func (lb *LevelBuilder) finish(ctx context.Context) (bool, *ProllyNode, cid.Cid, error) {
	if lb.done {
		return false, nil, cid.Undef, fmt.Errorf("repeated action")
	}
	lb.done = true

	// todo: deal with the cursor first

	// if not top level, finish pairs in buffer(if remaining)
	if lb.parentBuilder != nil {
		if lb.nodeBuffer.count() > 0 {
			if err := lb.splitBoundary(ctx); err != nil {
				return false, nil, cid.Undef, err
			}
		}
	} else {
		// if top level, get root node and cid

		// ending condition
		if lb.level == 0 || lb.nodeBuffer.count() > 1 {
			node, addr, err := buildAndSaveNode(ctx, lb.nodeBuffer, nil, lb.nodeStore)
			if err != nil {
				return false, nil, cid.Undef, err
			}
			return true, node, addr, nil
		} else {
			// top level but only a node with one pair, we should get canonical root
			trueRoot, rootCid, err := getCanonicalRoot(ctx, lb.nodeStore, lb.nodeBuffer)
			if err != nil {
				return false, nil, cid.Undef, err
			}
			return true, trueRoot, rootCid, nil
		}
	}

	// not arrive ending condition, so there is no root
	return false, nil, cid.Undef, nil
}

func getCanonicalRoot(ctx context.Context, ns types.NodeStore, nb *nodeBuilder) (*ProllyNode, cid.Cid, error) {
	if nb.count() != 1 {
		return nil, cid.Undef, fmt.Errorf("invalid nodeBuilder count")
	}
	childCid := nb.links[0]

	for {
		child, err := ns.ReadNode(ctx, childCid)
		if err != nil {
			return nil, cid.Undef, err
		}
		if child.IsLeaf() || child.ItemCount() > 1 {
			return child, childCid, nil
		}
		childCid = child.GetIdxLink(0)
	}
}

type Framework struct {
	done     bool
	builders []*LevelBuilder
}

func NewFramework(ctx context.Context, ns types.NodeStore, cfg *ChunkConfig, cur *Cursor) (*Framework, error,
) {
	cfgCid, err := ns.WriteTreeConfig(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}

	framework := &Framework{}

	leafBuilder := newLevelBuilder(ctx, 0, ns, cfg, cfgCid, cur, framework)

	builders := make([]*LevelBuilder, 0)
	builders = append(builders, leafBuilder)

	framework.builders = builders
	return framework, nil
}

func (fw *Framework) Append(ctx context.Context, key []byte, val ipld.Node) (bool, error) {
	if fw.done {
		return false, fmt.Errorf("append data in done framework")
	}
	return fw.builders[0].append(ctx, key, val, nil)
}

// AppendBatch should only use in data input, cannot be used in rebalance procedure
func (fw *Framework) AppendBatch(ctx context.Context, keys [][]byte, vals []ipld.Node) error {
	if fw.done {
		return fmt.Errorf("append data in done framework")
	}
	if len(keys) != len(vals) {
		return fmt.Errorf("keys and vals must have the same length")
	}
	for i := range keys {
		_, err := fw.Append(ctx, keys[i], vals[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (fw *Framework) finish(ctx context.Context) (*ProllyNode, cid.Cid, error) {
	if fw.done {
		return nil, cid.Undef, fmt.Errorf("repeated action")
	}
	fw.done = true

	var i int
	// finish all level builders and get the root node and cid
	for {
		// builders may be created while loop, so we need check it every time
		if i >= len(fw.builders) {
			return nil, cid.Undef, fmt.Errorf("finish all builders but not get root")
		}

		over, root, rootCid, err := fw.builders[i].finish(ctx)
		if err != nil {
			return nil, cid.Undef, err
		}
		if over {
			return root, rootCid, nil
		}

		i++
	}
}

func (fw *Framework) BuildTree(ctx context.Context) (*ProllyTree, error) {
	root, rootCid, err := fw.finish(ctx)
	if err != nil {
		return nil, err
	}

	tree := &ProllyTree{
		rootCid:    rootCid,
		root:       root,
		ns:         fw.builders[0].nodeStore,
		treeConfig: fw.builders[0].config,
	}

	fw.builders = nil

	return tree, nil
}

package tree

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	nodestore "go-ipld-prolly-trees/pkg/tree/node_store"
	. "go-ipld-prolly-trees/pkg/tree/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

type nodeBuffer struct {
	nd          ProllyNode
	nodeCoder   *nodestore.NodeCoder
	maxNodeSize int
	minNodeSize int
}

func (nb *nodeBuffer) count() int {
	return len(nb.nd.Keys)
}

func (nb *nodeBuffer) tryAddPair(key []byte, val ipld.Node) bool {
	nb.nd.Keys = append(nb.nd.Keys, key)
	nb.nd.Values = append(nb.nd.Values, val)

	sz := nb.encodedSize()
	if sz > nb.maxNodeSize {
		// revert
		count := nb.count()
		nb.nd.Keys = nb.nd.Keys[:count-1]
		nb.nd.Values = nb.nd.Values[:count-1]
		return false
	}

	return true
}

func (nb *nodeBuffer) encodedSize() int {
	ipldNode, err := nb.nd.ToNode()
	if err != nil {
		panic(err)
	}
	res, err := nb.nodeCoder.EncodeNode(ipldNode)
	if err != nil {
		panic(err)
	}

	return len(res)
}

func (nb *nodeBuffer) clean() {
	nb.nd.Keys = nil
	nb.nd.Values = nil
}

func (nb *nodeBuffer) build() *ProllyNode {
	node := nb.nd

	nb.clean()
	return &node
}

type LevelBuilder struct {
	config        *ChunkConfig
	isLeaf        bool
	cursor        *Cursor
	nodeBuffer    *nodeBuffer
	nodeCoder     *nodestore.NodeCoder
	cidprefix     *cid.Prefix
	parentBuilder *LevelBuilder
	nodeStore     types.NodeStore
	splitter      Splitter
	framework     *Framework
	done          bool
}

func newLevelBuilder(ctx context.Context, isLeaf bool, ns types.NodeStore, config *ChunkConfig, frameWork *Framework) (*LevelBuilder, error) {
	splitter := NewSplitterFromConfig(config)

	nb := &nodeBuffer{
		nd:          ProllyNode{IsLeaf: isLeaf},
		nodeCoder:   frameWork.nodeCoder,
		maxNodeSize: config.MaxNodeSize,
		minNodeSize: config.MinNodeSize,
	}

	lb := &LevelBuilder{
		config:     config,
		isLeaf:     isLeaf,
		nodeBuffer: nb,
		nodeStore:  ns,
		nodeCoder:  frameWork.nodeCoder,
		cidprefix:  frameWork.cidPrefix,
		splitter:   splitter,
		framework:  frameWork,
	}

	lb.framework.builders = append(lb.framework.builders, lb)

	return lb, nil
}

func newLevelBuilderWithCursor(ctx context.Context, isLeaf bool, ns types.NodeStore, config *ChunkConfig, frameWork *Framework, cur *Cursor) (*LevelBuilder, error) {
	if cur == nil {
		return nil, fmt.Errorf("nil cursor")
	}
	lb, err := newLevelBuilder(ctx, isLeaf, ns, config, frameWork)
	if err != nil {
		return nil, err
	}
	lb.cursor = cur
	err = lb.appendEntriesBeforeCursor(ctx)
	if err != nil {
		return nil, err
	}

	if cur.parent != nil {
		err = lb.createParentLevelBuilder(ctx)
		if err != nil {
			return nil, err
		}
	}

	return lb, nil
}

func (lb *LevelBuilder) append(ctx context.Context, key []byte, value ipld.Node) (bool, error) {
	if lb.done {
		return false, fmt.Errorf("append pair in done builder")
	}

	valBytes, err := lb.nodeCoder.EncodeNode(value)
	if err != nil {
		return false, err
	}

	ok := lb.nodeBuffer.tryAddPair(key, value)
	if !ok {
		err = lb.splitBoundary(ctx)
		if err != nil {
			return false, err
		}
		ok = lb.nodeBuffer.tryAddPair(key, value)
		if !ok {
			panic("too large pair bigger than the node size limit")
		}
	}

	err = lb.splitter.Append(key, valBytes)
	if err != nil {
		return false, err
	}

	// boundary is true , but it's branch node with only one pair k/v, so we can not split here.
	// if split in the state, it will generate boundary infinitely(its parent node will generate in the same state too)
	if lb.splitter.IsBoundary() && !(!lb.isLeaf && lb.nodeBuffer.count() == 1) {
		err = lb.splitBoundary(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func buildAndSaveNode(ctx context.Context, nb *nodeBuffer, prefix *cid.Prefix, ns types.NodeStore) (*ProllyNode, cid.Cid, error) {
	if !(nb.count() > 0) {
		return nil, cid.Undef, fmt.Errorf("invalid nodeBuffer to build")
	}
	node := nb.build()
	addr, err := ns.WriteNode(ctx, node, prefix)
	if err != nil {
		return nil, cid.Undef, err
	}
	return node, addr, nil
}

func (lb *LevelBuilder) splitBoundary(ctx context.Context) error {
	node, addr, err := buildAndSaveNode(ctx, lb.nodeBuffer, lb.cidprefix, lb.nodeStore)
	if err != nil {
		return err
	}

	key := node.GetIdxKey(node.ItemCount() - 1)
	lastKey := make([]byte, len(key))
	copy(lastKey, key)

	if lb.parentBuilder == nil {
		err = lb.createParentLevelBuilder(ctx)
		if err != nil {
			return err
		}
	}

	vnode := basicnode.NewLink(cidlink.Link{Cid: addr})
	_, err = lb.parentBuilder.append(ctx, lastKey, vnode)
	lb.splitter.Reset()

	return nil
}

func (lb *LevelBuilder) createParentLevelBuilder(ctx context.Context) error {
	if lb.parentBuilder != nil {
		panic("invalid action, parent level builder has already existed!")
	}

	var err error
	var parentCursor *Cursor

	if lb.cursor != nil && lb.cursor.parent != nil {
		parentCursor = lb.cursor.parent
	}

	if parentCursor != nil {
		lb.parentBuilder, err = newLevelBuilderWithCursor(ctx, false, lb.nodeStore, lb.config, lb.framework, parentCursor)
	} else {
		lb.parentBuilder, err = newLevelBuilder(ctx, false, lb.nodeStore, lb.config, lb.framework)
	}
	if err != nil {
		return err
	}

	return nil
}

func (lb *LevelBuilder) appendEntriesBeforeCursor(ctx context.Context) error {
	if lb.cursor == nil {
		panic("invalid action")
	}

	index := 0
	for index < lb.cursor.idx {
		_, err := lb.append(ctx,
			lb.cursor.node.GetIdxKey(index),
			lb.cursor.node.GetIdxValue(index),
		)
		if err != nil {
			return err
		}
		index++
	}

	return nil
}

func (lb *LevelBuilder) appendEntriesAfterCursor(ctx context.Context) error {
	cur := lb.cursor
	for lb.cursor.IsValid() {
		boundaryGenerated, err := lb.append(ctx,
			cur.GetKey(),
			cur.GetValue(),
		)
		if err != nil {
			return err
		}
		if boundaryGenerated && cur.IsAtEnd() {
			// same boundary generated in new node
			break
		}

		err = cur.Advance()
		if err != nil {
			return err
		}
	}

	if cur.parent != nil {
		// the modified path should not append into levelBuilder
		err := cur.parent.Advance()
		if err != nil {
			return err
		}

		cur.node = nil
	}

	return nil
}

func (lb *LevelBuilder) finish(ctx context.Context) (bool, *ProllyNode, cid.Cid, error) {
	if lb.done {
		return false, nil, cid.Undef, fmt.Errorf("repeated action")
	}
	defer func() {
		lb.done = true
	}()

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
		if lb.isLeaf || lb.nodeBuffer.count() > 1 {
			node, addr, err := buildAndSaveNode(ctx, lb.nodeBuffer, lb.cidprefix, lb.nodeStore)
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

func getCidFromIpldNode(n ipld.Node) cid.Cid {
	link, err := n.AsLink()
	if err != nil {
		panic(fmt.Errorf("invalid value, expected cidlink, got: %v", n))
	}
	return link.(cidlink.Link).Cid
}

func getCanonicalRoot(ctx context.Context, ns types.NodeStore, nb *nodeBuffer) (*ProllyNode, cid.Cid, error) {
	if nb.count() != 1 {
		return nil, cid.Undef, fmt.Errorf("invalid nodeBuffer count")
	}
	childCid := getCidFromIpldNode(nb.nd.Values[0])

	for {
		child, err := ns.ReadNode(ctx, childCid)
		if err != nil {
			return nil, cid.Undef, err
		}
		if child.IsLeafNode() || child.ItemCount() > 1 {
			return child, childCid, nil
		}
		childCid = child.GetIdxLink(0)
	}
}

type Framework struct {
	done      bool
	cidPrefix *cid.Prefix
	nodeCoder *nodestore.NodeCoder
	configCid cid.Cid
	builders  []*LevelBuilder
}

func NewFramework(ctx context.Context, ns types.NodeStore, cfg *ChunkConfig, cur *Cursor) (*Framework, error,
) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	cidprefix, err := cid.PrefixFromBytes(cfg.NodeCodec)
	if err != nil {
		return nil, err
	}
	nodeCoder := nodestore.NewNodeCoder()
	// ignore error, we can register the codec later
	_ = nodeCoder.InitEncoder(cidprefix.Codec)

	configCid, err := ns.WriteTreeConfig(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}

	framework := &Framework{
		configCid: configCid,
		cidPrefix: &cidprefix,
		nodeCoder: nodeCoder,
	}

	if cur == nil {
		_, err = newLevelBuilder(ctx, true, ns, cfg, framework)
	} else {
		_, err = newLevelBuilderWithCursor(ctx, true, ns, cfg, framework, cur)
	}

	return framework, nil
}

func (fw *Framework) Append(ctx context.Context, key []byte, val ipld.Node) error {
	if fw.done {
		return fmt.Errorf("append data in done framework")
	}
	_, err := fw.builders[0].append(ctx, key, val)
	return err
}

func (fw *Framework) AdvanceCursor(ctx context.Context) error {
	if fw.builders[0].cursor == nil {
		return fmt.Errorf("nil cursor to advance")
	}
	return fw.builders[0].cursor.Advance()
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
		err := fw.Append(ctx, keys[i], vals[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (fw *Framework) finish(ctx context.Context) (*ProllyNode, *ProllyTreeNode, error) {
	if fw.done {
		return nil, nil, fmt.Errorf("repeated action")
	}
	fw.done = true

	var i int
	// finish all level builders and get the root node and cid
	for {
		// builders may be created while loop, so we need check it every time
		if i >= len(fw.builders) {
			return nil, nil, fmt.Errorf("finish all builders but not get root")
		}

		levelBuilder := fw.builders[i]
		if levelBuilder.cursor != nil {
			err := levelBuilder.appendEntriesAfterCursor(ctx)
			if err != nil {
				return nil, nil, err
			}
		}

		over, rootNode, rootCid, err := levelBuilder.finish(ctx)
		if err != nil {
			return nil, nil, err
		}
		if over {
			root := &ProllyTreeNode{
				RootCid:   rootCid,
				ConfigCid: fw.configCid,
			}
			return rootNode, root, nil
		}

		i++
	}
}

func (fw *Framework) BuildTree(ctx context.Context) (*ProllyTree, error) {
	rootNode, prollyTreeNode, err := fw.finish(ctx)
	if err != nil {
		return nil, err
	}
	treeNodeCid, err := fw.builders[0].nodeStore.WriteRoot(ctx, prollyTreeNode, nil)
	if err != nil {
		return nil, err
	}

	tree := &ProllyTree{
		treeCid:    treeNodeCid,
		rootCid:    prollyTreeNode.RootCid,
		root:       rootNode,
		ns:         fw.builders[0].nodeStore,
		treeConfig: fw.builders[0].config,
	}

	fw.builders = nil

	return tree, nil
}

func (fw *Framework) appendToCursor(ctx context.Context, cur *Cursor) error {
	return fw.builders[0].appendToCursor(ctx, cur)
}

func (lb *LevelBuilder) appendToCursor(ctx context.Context, cur *Cursor) error {
	lcur := lb.cursor
	if lcur == nil || cur == nil {
		return fmt.Errorf("invalid cursor")
	}

	if lcur.Equal(cur) {
		return nil
	} else if lcur.Compare(cur) > 0 {
		return fmt.Errorf("cursor is behind the framework")
	}

	boundary, err := lb.append(ctx, lcur.GetKey(), lcur.GetValue())
	if err != nil {
		return err
	}

	for {
		if boundary && lcur.IsAtEnd() {
			break
		}
		err = lcur.Advance()
		if err != nil {
			return err
		}
		if lcur.Compare(cur) == 0 {
			return nil
		}

		boundary, err = lb.append(ctx, lcur.GetKey(), lcur.GetValue())
		if err != nil {
			return err
		}
	}

	if lcur.parent == nil && cur.parent == nil {
		return nil
	} else if lcur.parent != nil && cur.parent != nil {
		if lcur.parent.Equal(cur.parent) {
			return nil
		}
	} else {
		return fmt.Errorf("two cursors has different height")
	}

	err = lb.parentBuilder.appendToCursor(ctx, cur.parent)
	if err != nil {
		return err
	}

	lb.cursor.node = cur.node
	lb.cursor.idx = cur.idx

	err = lb.appendEntriesBeforeCursor(ctx)
	if err != nil {
		return err
	}

	return nil
}

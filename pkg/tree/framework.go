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
	configCid     cid.Cid
	level         int
	cursor        *Cursor
	nodeBuffer    *nodeBuilder
	parentBuilder *LevelBuilder
	nodeStore     types.NodeStore
	splitter      Splitter
	framework     *Framework
	done          bool
}

func newLevelBuilder(ctx context.Context, level int, ns types.NodeStore, config *ChunkConfig, configCid cid.Cid, cur *Cursor) *LevelBuilder {
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
	}

	return lb
}

func (lb *LevelBuilder) append(ctx context.Context, key []byte, leafValue ipld.Node, link *cid.Cid) (bool, error) {
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

func (lb *LevelBuilder) splitBoundary(ctx context.Context) error {
	if lb.nodeBuffer.count() <= 0 {
		return fmt.Errorf("invalid items count")
	}
	node := lb.nodeBuffer.build()
	addr, err := lb.nodeStore.WriteNode(ctx, node, nil)
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

	_, err = lb.parentBuilder.append(ctx, lastKey, nil, &addr)
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

	lb.parentBuilder = newLevelBuilder(ctx, lb.level+1, lb.nodeStore, lb.config, lb.configCid, parentCursor)
	return err
}

type Framework struct {
	done      bool
	builders  []*LevelBuilder
	config    *ChunkConfig
	configCid cid.Cid
	ns        types.NodeStore
}

func NewFramework(ctx context.Context, ns types.NodeStore, cfg *ChunkConfig, cur *Cursor) (*Framework, error,
) {
	cfgCid, err := ns.WriteTreeConfig(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}

	leafBuilder := newLevelBuilder(ctx, 0, ns, cfg, cfgCid, nil)

	builders := make([]*LevelBuilder, 0)
	builders = append(builders, leafBuilder)

	return &Framework{
		builders:  builders,
		config:    cfg,
		configCid: cfgCid,
		ns:        ns,
	}, nil
}

func (fw *Framework) Append(ctx context.Context, key []byte, val ipld.Node) (bool, error) {

	return fw.builders[0].append(ctx, key, val, nil)
}

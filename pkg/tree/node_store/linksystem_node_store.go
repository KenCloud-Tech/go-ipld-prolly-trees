package nodestore

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	. "go-ipld-prolly-trees/pkg/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

var _ types.NodeStore = &LinkSystemNodeStore{}

type LinkSystemNodeStore struct {
	lsys *linking.LinkSystem
}

func NewLinkSystemNodeStore(lsys *linking.LinkSystem) *LinkSystemNodeStore {
	return &LinkSystemNodeStore{lsys: lsys}
}

func (ns *LinkSystemNodeStore) WriteNode(ctx context.Context, nd *ProllyNode, prefix *cid.Prefix) (cid.Cid, error) {
	var linkProto cidlink.LinkPrototype
	if prefix == nil {
		// default linkproto
		linkProto = DefaultLinkProto
	} else {
		linkProto = cidlink.LinkPrototype{Prefix: *prefix}
	}
	ipldNode, err := nd.ToNode()
	if err != nil {
		return cid.Undef, err
	}
	lnk, err := ns.lsys.Store(ipld.LinkContext{Ctx: ctx}, linkProto, ipldNode)
	if err != nil {
		return cid.Undef, err
	}
	c := lnk.(cidlink.Link).Cid

	return c, nil
}

func (ns *LinkSystemNodeStore) ReadNode(ctx context.Context, c cid.Cid) (*ProllyNode, error) {
	nd, err := ns.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, ProllyNodePrototype.Representation())
	if err != nil {
		return nil, err
	}

	inode, err := UnwrapProllyNode(nd)
	if err != nil {
		return nil, err
	}

	return inode, nil
}

func (ns *LinkSystemNodeStore) WriteTreeNode(ctx context.Context, root *ProllyTreeNode, prefix *cid.Prefix) (cid.Cid, error) {
	var linkProto cidlink.LinkPrototype
	if prefix == nil {
		// default linkproto
		linkProto = DefaultLinkProto
	} else {
		linkProto = cidlink.LinkPrototype{Prefix: *prefix}
	}
	ipldNode, err := root.ToNode()
	if err != nil {
		return cid.Undef, err
	}
	lnk, err := ns.lsys.Store(ipld.LinkContext{Ctx: ctx}, linkProto, ipldNode)
	if err != nil {
		return cid.Undef, err
	}
	c := lnk.(cidlink.Link).Cid

	return c, nil
}

func (ns *LinkSystemNodeStore) ReadTreeNode(ctx context.Context, c cid.Cid) (*ProllyTreeNode, error) {
	nd, err := ns.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, ProllyTreePrototype.Representation())
	if err != nil {
		return nil, err
	}

	root, err := UnwrapProllyRoot(nd)
	if err != nil {
		return nil, err
	}

	return root, nil
}

func (ns *LinkSystemNodeStore) ReadTreeConfig(ctx context.Context, c cid.Cid) (*TreeConfig, error) {
	icfg, err := ns.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, ChunkConfigPrototype.Representation())
	if err != nil {
		return nil, err
	}

	cfg, err := UnwrapChunkConfig(icfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (ns *LinkSystemNodeStore) WriteTreeConfig(ctx context.Context, cfg *TreeConfig, prefix *cid.Prefix) (cid.Cid, error) {
	var linkProto cidlink.LinkPrototype
	if prefix == nil {
		// default linkproto
		linkProto = DefaultLinkProto
	} else {
		linkProto = cidlink.LinkPrototype{Prefix: *prefix}
	}

	ipldNode, err := cfg.ToNode()
	if err != nil {
		return cid.Undef, err
	}
	lnk, err := ns.lsys.Store(ipld.LinkContext{Ctx: ctx}, linkProto, ipldNode)
	if err != nil {
		return cid.Undef, err
	}
	c := lnk.(cidlink.Link).Cid

	return c, nil
}

func (ns *LinkSystemNodeStore) Close() {
}

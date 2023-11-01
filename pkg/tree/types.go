package tree

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
)

// todo: maybe we need clean orphan nodes regularly?
type NodeStore interface {
	WriteNode(ctx context.Context, nd *ProllyNode, prefix *cid.Prefix) (cid.Cid, error)
	ReadNode(ctx context.Context, c cid.Cid) (*ProllyNode, error)

	WriteTree(ctx context.Context, tree *ProllyTree, prefix *cid.Prefix) (cid.Cid, error)
	ReadTree(ctx context.Context, c cid.Cid) (*ProllyTree, error)

	WriteTreeConfig(ctx context.Context, cfg *TreeConfig, prefix *cid.Prefix) (cid.Cid, error)
	ReadTreeConfig(ctx context.Context, c cid.Cid) (*TreeConfig, error)

	WriteProof(ctx context.Context, prf Proof, prefix *cid.Prefix) (cid.Cid, error)
	ReadProof(ctx context.Context, c cid.Cid) (Proof, error)

	LinkSystem() *ipld.LinkSystem

	Close()
}

var DefaultLinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 20,
	},
}

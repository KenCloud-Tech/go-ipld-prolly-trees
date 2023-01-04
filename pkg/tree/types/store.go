package types

import (
	"context"
	"github.com/ipfs/go-cid"
	"go-ipld-prolly-trees/pkg/schema"
)

// todo: maybe we need clean orphan nodes regularly?
type NodeStore interface {
	WriteNode(ctx context.Context, nd *schema.ProllyNode, prefix *cid.Prefix) (cid.Cid, error)
	ReadNode(ctx context.Context, c cid.Cid) (*schema.ProllyNode, error)

	WriteTreeNode(ctx context.Context, root *schema.ProllyTreeNode, prefix *cid.Prefix) (cid.Cid, error)
	ReadTreeNode(ctx context.Context, c cid.Cid) (*schema.ProllyTreeNode, error)

	WriteTreeConfig(ctx context.Context, cfg *schema.TreeConfig, prefix *cid.Prefix) (cid.Cid, error)
	ReadTreeConfig(ctx context.Context, c cid.Cid) (*schema.TreeConfig, error)

	Close()
}

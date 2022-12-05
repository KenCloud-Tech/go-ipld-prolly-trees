package types

import (
	"context"
	"github.com/ipfs/go-cid"
	"go-ipld-prolly-trees/pkg/tree/schema"
)

// todo: maybe we need clean orphan nodes regularly?
type NodeStore interface {
	WriteNode(ctx context.Context, nd *schema.ProllyNode, prefix *cid.Prefix) (cid.Cid, error)
	ReadNode(ctx context.Context, c cid.Cid) (*schema.ProllyNode, error)
	WriteTreeConfig(ctx context.Context, cfg *schema.ChunkConfig, prefix *cid.Prefix) (cid.Cid, error)
	ReadTreeConfig(ctx context.Context, c cid.Cid) (*schema.ChunkConfig, error)
	Close()
}

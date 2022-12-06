package nodestore

import (
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/assert"
	"go-ipld-prolly-trees/pkg/tree/schema"
	"testing"
)

func TestIPLDNodeStoreLoad(t *testing.T) {
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	ns, err := NewNodeStore(bs, &StoreConfig{CacheSize: 1 << 10})
	assert.NoError(t, err)

	c1, err := DefaultLinkProto.Sum([]byte("link1"))
	assert.NoError(t, err)
	cfg := schema.DefaultChunkConfig()
	cfgCid, err := ns.WriteTreeConfig(context.Background(), cfg, nil)
	assert.NoError(t, err)

	vnode := basicnode.NewBytes([]byte("123v"))
	nd := &schema.ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: []ipld.Node{vnode},
		Links:  []cid.Cid{c1},
		Level:  199998,
		Config: cfgCid,
	}

	ctx := context.Background()

	c, err := ns.WriteNode(ctx, nd, nil)
	assert.NoError(t, err)

	inode, err := ns.ReadNode(ctx, c)
	assert.NoError(t, err)

	_cfg, err := ns.ReadTreeConfig(context.Background(), inode.Config)
	assert.NoError(t, err)

	assert.Equal(t, nd.Keys, inode.Keys)
	assert.Equal(t, nd.Values, inode.Values)
	assert.Equal(t, nd.Level, inode.Level)
	assert.Equal(t, nd.Config, inode.Config)
	assert.True(t, _cfg.Equal(cfg))
}

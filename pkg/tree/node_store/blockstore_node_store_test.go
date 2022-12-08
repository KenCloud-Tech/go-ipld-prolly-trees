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

	assert.NoError(t, err)

	vnode := basicnode.NewBytes([]byte("123v"))
	nd := &schema.ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: []ipld.Node{vnode},
		IsLeaf: true,
	}

	ctx := context.Background()

	c, err := ns.WriteNode(ctx, nd, nil)
	assert.NoError(t, err)

	inode, err := ns.ReadNode(ctx, c)
	assert.NoError(t, err)

	assert.NoError(t, err)

	assert.Equal(t, nd.Keys, inode.Keys)
	assert.Equal(t, nd.Values, inode.Values)
	assert.Equal(t, nd.IsLeafNode(), inode.IsLeafNode())
}

func TestIPLD(t *testing.T) {
	pre := DefaultLinkProto.Prefix.Bytes()

	pre2 := cid.Prefix{
		Version:  1,
		Codec:    321321312312,
		MhType:   3213213123,
		MhLength: 16231321,
	}

	t.Log(len(pre))
	t.Log(len(pre2.Bytes()))
}

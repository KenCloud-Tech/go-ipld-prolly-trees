package tree

import (
	"bytes"
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mcodec "github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
	"testing"
)

var prollyNode = &ProllyNode{
	Keys: [][]byte{
		[]byte("123k"),
		[]byte("1234k"),
	},
	Values: []ipld.Node{
		basicnode.NewBytes([]byte("123v")),
		basicnode.NewBytes([]byte("1234v")),
	},
	IsLeaf: true,
}

func TestNodeStoreLoad(t *testing.T) {
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	ns, err := NewBlockNodeStore(bs, &StoreConfig{CacheSize: 1 << 10})
	assert.NoError(t, err)

	ctx := context.Background()

	c, err := ns.WriteNode(ctx, prollyNode, nil)
	assert.NoError(t, err)

	inode, err := ns.ReadNode(ctx, c)
	assert.NoError(t, err)

	ipldNd, err := prollyNode.ToNode()
	assert.NoError(t, err)

	bk, err := ns.bs.Get(ctx, c)
	assert.NoError(t, err)
	t.Log(len(bk.RawData()))
	t.Log(string(bk.RawData()))
	buf := new(bytes.Buffer)
	err = dagcbor.Encode(ipldNd, buf)
	assert.NoError(t, err)
	t.Log(len(buf.Bytes()))
	t.Log(buf.String())

	assert.Equal(t, buf.Len(), len(bk.RawData()))

	assert.NoError(t, err)

	assert.Equal(t, prollyNode.Keys, inode.Keys)
	assert.Equal(t, prollyNode.Values, inode.Values)
	assert.Equal(t, prollyNode.IsLeafNode(), inode.IsLeafNode())

	cfg := DefaultChunkConfig()
	c, err = ns.WriteTreeConfig(ctx, cfg, nil)
	assert.NoError(t, err)

	cid1, _ := DefaultLinkProto.Sum([]byte("testdata1"))
	cid2, _ := DefaultLinkProto.Sum([]byte("testdata2"))
	ps1 := &ProofSegment{
		Node:  cid1,
		Index: 0,
	}
	ps2 := &ProofSegment{
		Node:  cid2,
		Index: 1,
	}
	prf := Proof{*ps1, *ps2}
	c, err = ns.WriteProof(ctx, prf, nil)
	assert.NoError(t, err)

	rePrf, err := ns.ReadProof(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, prf, rePrf)
}

func TestCidPrefixAndEncoder(t *testing.T) {
	ns := TestMemNodeStore()
	ipldNode, err := prollyNode.ToNode()
	assert.NoError(t, err)

	prefix := DefaultLinkProto.Prefix

	_, err = ns.WriteNode(context.Background(), prollyNode, &prefix)
	assert.NoError(t, err)

	encoder, err := multicodec.DefaultRegistry.LookupEncoder(prefix.Codec)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	err = encoder(ipldNode, buf)
	assert.NoError(t, err)

	prefix2 := cid.Prefix{
		Version:  1,
		Codec:    uint64(mcodec.DagJson),
		MhType:   uint64(mcodec.Sha2_224),
		MhLength: 0,
	}

	encoder, err = multicodec.DefaultRegistry.LookupEncoder(prefix2.Codec)
	assert.NoError(t, err)

	buf = new(bytes.Buffer)
	err = encoder(ipldNode, buf)
	assert.NoError(t, err)

	_, err = ns.WriteNode(context.Background(), prollyNode, &prefix2)
	assert.NoError(t, err)

}

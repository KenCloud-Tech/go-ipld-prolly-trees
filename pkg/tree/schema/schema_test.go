package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	"github.com/zeebo/assert"
	"testing"
)

var defaultLinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 16,
	},
}

func TestGenIPLDNode(t *testing.T) {
	cid1, _ := defaultLinkProto.Sum([]byte("123v"))

	vnode := basicnode.NewBytes([]byte("12sdsada"))

	nd := &ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: []ipld.Node{vnode},
		Links:  []cid.Cid{cid1},
		Level:  0,
		Config: cid.Undef,
	}

	_, err := nd.ToNode()
	assert.NoError(t, err)

	cfg := DefaultChunkConfig()
	_, err = cfg.ToNode()
	assert.NoError(t, err)
}

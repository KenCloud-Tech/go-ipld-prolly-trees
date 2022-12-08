package schema

import (
	"bytes"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
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

	vnode := basicnode.NewBytes([]byte("12sdsada"))

	nd := &ProllyNode{
		Keys:   [][]byte{[]byte("123k")},
		Values: []ipld.Node{vnode},
		IsLeaf: true,
	}

	_, err := nd.ToNode()
	assert.NoError(t, err)

	cfg := DefaultChunkConfig()
	_, err = cfg.ToNode()
	assert.NoError(t, err)

	node, err := nd.ToNode()
	assert.NoError(t, err)
	buf := new(bytes.Buffer)
	err = dagcbor.Encode(node, buf)
	assert.NoError(t, err)
}

func TestNodeSize(t *testing.T) {
	vnode := basicnode.NewBytes([]byte("12sdsada"))
	vnode2 := basicnode.NewBytes([]byte("123sdsada"))

	nd := &ProllyNode{
		Keys:   [][]byte{[]byte("123k"), []byte("1234k")},
		Values: []ipld.Node{vnode, vnode2},
		IsLeaf: true,
	}

	buf := new(bytes.Buffer)
	vnd, err := nd.ToNode()
	assert.NoError(t, err)
	err = dagcbor.Encode(vnd, buf)
	assert.NoError(t, err)

	t.Log(len(buf.Bytes()))
	t.Log(buf.Bytes())
	t.Log(buf.String())

	tnd := &TProllyNode{
		IsLeaf: true,
		Pairs:  []Entry{{Key: []byte("123k"), Value: vnode}, {Key: []byte("1234k"), Value: vnode2}},
	}
	buf2 := new(bytes.Buffer)
	vnd2, err := tnd.ToNode()
	assert.NoError(t, err)
	err = dagcbor.Encode(vnd2, buf2)
	assert.NoError(t, err)

	t.Log(len(buf2.Bytes()))
	t.Log(buf2.Bytes())
	t.Log(buf2.String())

}

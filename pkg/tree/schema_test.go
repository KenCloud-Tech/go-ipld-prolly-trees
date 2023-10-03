package tree

import (
	"bytes"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/zeebo/assert"
	"testing"
)

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

func TestGenProof(t *testing.T) {
	cid1, _ := DefaultLinkProto.Sum([]byte("testdata1"))
	ps := &ProofSegment{
		Node:  cid1,
		Index: 0,
	}
	n, err := ps.ToNode()
	assert.NoError(t, err)
	t.Log(n)

	rePs, err := UnwrapProofSegment(n)
	assert.NoError(t, err)
	assert.Equal(t, rePs.Index, ps.Index)
	assert.Equal(t, rePs.Node, ps.Node)

	pf := Proof{*ps}

	pn, err := pf.ToNode()
	assert.NoError(t, err)
	t.Logf("%#v", pn)

	reProof, err := UnwrapProof(pn)
	assert.NoError(t, err)
	assert.Equal(t, (*reProof)[0].Node, cid1)
	assert.Equal(t, (*reProof)[0].Index, 0)
}

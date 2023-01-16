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

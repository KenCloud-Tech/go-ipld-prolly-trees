package tree

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

//func TestNodeSize(t *testing.T) {
//
//	testKeys, testVals := tree.RandomTestData(1000)
//
//	nd := &ProllyNode{
//		IsLeaf: true,
//	}
//
//	for i := range testKeys{
//		nd.Keys = append(nd.Keys, testKeys[i])
//		nd.Values = append(nd.Values, testVals[i])
//	}
//
//	buf := new(bytes.Buffer)
//	vnd, err := nd.ToNode()
//	assert.NoError(t, err)
//	err = dagcbor.Encode(vnd, buf)
//	assert.NoError(t, err)
//
//	t.Log(len(buf.Bytes()))
//	t.Log(buf.Bytes())
//	t.Log(buf.String())
//
//	tnd := &TProllyNode{
//		IsLeaf: true,
//	}
//
//	for i := range testKeys{
//		entry := Entry{
//			Key:   testKeys[i],
//			Value: testVals[i],
//		}
//		tnd.Pairs = append(tnd.Pairs, entry)
//	}
//
//
//	buf2 := new(bytes.Buffer)
//	vnd2, err := tnd.ToNode()
//	assert.NoError(t, err)
//	err = dagcbor.Encode(vnd2, buf2)
//	assert.NoError(t, err)
//
//	t.Log(len(buf2.Bytes()))
//	t.Log(buf2.Bytes())
//	t.Log(buf2.String())
//
//}

package nodestore

import (
	mcodec "github.com/multiformats/go-multicodec"
	"github.com/zeebo/assert"
	"go-ipld-prolly-trees/pkg/schema"
	"testing"
)

func TestNodeCoder(t *testing.T) {
	cd := NewNodeCoder()
	err := cd.InitEncoder(schema.DefaultLinkProto.Codec)
	assert.NoError(t, err)
	ipldNd, err := prollyNode.ToNode()
	assert.NoError(t, err)
	res, err := cd.EncodeNode(ipldNd)
	assert.NoError(t, err)
	t.Log(string(res))
	err = cd.InitEncoder(uint64(mcodec.DagJson))
	assert.NoError(t, err)
	res, err = cd.EncodeNode(ipldNd)
	assert.NoError(t, err)
	t.Log(string(res))

}

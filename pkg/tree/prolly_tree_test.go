package tree

import (
	"context"
	"github.com/zeebo/assert"
	"go-ipld-prolly-trees/pkg/tree/schema"
	"math/rand"
	"testing"
)

func TestProllyTreeBuildAndReload(t *testing.T) {
	ctx := context.Background()
	ns := testMemNodeStore()
	cfg := schema.DefaultChunkConfig()
	framwork, err := NewFramework(ctx, ns, cfg, nil)
	assert.NoError(t, err)

	testKeys, testVals := RandomTestData(100000)
	err = framwork.AppendBatch(ctx, testKeys, testVals)
	assert.NoError(t, err)
	tree, rootCid, err := framwork.BuildTree(ctx)
	assert.NoError(t, err)

	for i := 0; i < 100000; i++ {
		idx := rand.Intn(100000)
		val, err := tree.Get(testKeys[idx])
		assert.NoError(t, err)
		vnode, _ := val.AsBytes()
		trueVnode, _ := testVals[idx].AsBytes()
		assert.Equal(t, vnode, trueVnode)
	}

	reloadTree, err := LoadProllyTreeFromRootCid(rootCid, ns)
	assert.NoError(t, err)
	for i := 0; i < 100000; i++ {
		idx := rand.Intn(100000)
		val, err := reloadTree.Get(testKeys[idx])
		assert.NoError(t, err)
		vnode, _ := val.AsBytes()
		trueVnode, _ := testVals[idx].AsBytes()
		assert.Equal(t, vnode, trueVnode)
	}

}

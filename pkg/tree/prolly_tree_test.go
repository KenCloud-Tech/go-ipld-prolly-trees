package tree

import (
	"context"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/zeebo/assert"
	"go-ipld-prolly-trees/pkg/tree/node_store"
	"go-ipld-prolly-trees/pkg/tree/schema"
	"math/rand"
	"testing"
)

func TestProllyTreeBuildAndReload(t *testing.T) {
	ctx := context.Background()
	ns := nodestore.TestMemNodeStore()
	cfg := schema.DefaultChunkConfig()
	framwork, err := NewFramework(ctx, ns, cfg, nil)
	assert.NoError(t, err)

	testKeys, testVals := RandomTestData(100000)
	err = framwork.AppendBatch(ctx, testKeys, testVals)
	assert.NoError(t, err)
	tree, err := framwork.BuildTree(ctx)
	assert.NoError(t, err)
	oldTreeCid := tree.treeCid

	for i := 0; i < 100000; i++ {
		idx := rand.Intn(100000)
		val, err := tree.Get(testKeys[idx])
		assert.NoError(t, err)
		vnode, _ := val.AsBytes()
		trueVnode, _ := testVals[idx].AsBytes()
		assert.Equal(t, vnode, trueVnode)
	}

	newValNode := basicnode.NewBytes([]byte("test new values！!aAbB"))
	err = tree.Mutate()
	assert.NoError(t, err)

	err = tree.Put(ctx, testKeys[19999], newValNode)
	assert.NoError(t, err)

	err = tree.Rebuild(ctx)
	assert.NoError(t, err)

	reloadTree, err := LoadProllyTreeFromRootCid(tree.treeCid, ns)
	assert.NoError(t, err)
	for i := 0; i < 100000; i++ {
		idx := rand.Intn(100000)
		if idx == 19999 {
			continue
		}
		val, err := reloadTree.Get(testKeys[idx])
		assert.NoError(t, err)
		vnode, _ := val.AsBytes()
		trueVnode, _ := testVals[idx].AsBytes()
		assert.Equal(t, vnode, trueVnode)
	}

	// new
	val, err := reloadTree.Get(testKeys[19999])
	assert.NoError(t, err)
	vnode, _ := val.AsBytes()
	trueVnode, _ := newValNode.AsBytes()
	assert.Equal(t, vnode, trueVnode)

	// old
	reloadOldTree, err := LoadProllyTreeFromRootCid(oldTreeCid, ns)
	assert.NoError(t, err)
	val, err = reloadOldTree.Get(testKeys[19999])
	assert.NoError(t, err)
	vnode, _ = val.AsBytes()
	trueVnode, _ = testVals[19999].AsBytes()
	assert.Equal(t, vnode, trueVnode)

	// insert
	insertVnode := basicnode.NewString("dasdsadasdsad")
	err = tree.Mutate()
	assert.NoError(t, err)
	err = tree.Put(ctx, []byte("testkey123321"), insertVnode)
	assert.Equal(t, vnode, trueVnode)
	err = tree.Rebuild(ctx)
	assert.NoError(t, err)

	res, err := tree.Get([]byte("testkey123321"))
	assert.NoError(t, err)
	expectStr, _ := insertVnode.AsString()
	resStr, _ := res.AsString()
	assert.Equal(t, expectStr, resStr)
}

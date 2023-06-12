package fixtures

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	car2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/zeebo/assert"
	"github.com/kenlabs/go-ipld-prolly-trees/pkg/tree"
	"os"
	"testing"
)

func TestGenIPLDData(t *testing.T) {
	count := 10000
	ctx := context.Background()
	testKeys, testVals := tree.RandomTestData(count)
	builder := basicnode.Prototype.Map.NewBuilder()
	ma, err := builder.BeginMap(int64(count))
	assert.NoError(t, err)
	for i := range testKeys {
		assert.NoError(t, ma.AssembleKey().AssignString(string(testKeys[i])))
		assert.NoError(t, ma.AssembleValue().AssignNode(testVals[i]))
	}

	assert.NoError(t, ma.Finish())
	dataNode := builder.Build()

	// the origin node keep order of pairs but will shuffle after serialize(deserialize)
	iter := dataNode.MapIterator()
	i := 0
	for !iter.Done() {
		k, v, err := iter.Next()
		assert.NoError(t, err)
		kString, err := k.AsString()
		assert.NoError(t, err)
		assert.Equal(t, []byte(kString), testKeys[i])
		vBytes, _ := v.AsBytes()
		tBytes, _ := testVals[i].AsBytes()
		assert.Equal(t, vBytes, tBytes)
		i++
	}

	ns := tree.TestMemNodeStore()
	lnk, err := ns.LinkSystem().Store(ipld.LinkContext{Ctx: ctx}, tree.DefaultLinkProto, dataNode)
	assert.NoError(t, err)
	dataNodeCid := lnk.(cidlink.Link).Cid
	t.Log(dataNodeCid.String())

	buf := new(bytes.Buffer)
	_, err = car2.TraverseV1(context.Background(), ns.LinkSystem(), dataNodeCid, selectorparse.CommonSelector_ExploreAllRecursively, buf)
	assert.NoError(t, err)
}

func genFixtures(count int, t *testing.T) {
	ctx := context.Background()
	testKeys, testVals := tree.RandomTestData(count)
	builder := basicnode.Prototype.Map.NewBuilder()
	ma, err := builder.BeginMap(int64(count))
	assert.NoError(t, err)
	for i := range testKeys {
		assert.NoError(t, ma.AssembleKey().AssignString(string(testKeys[i])))
		assert.NoError(t, ma.AssembleValue().AssignNode(testVals[i]))
	}

	assert.NoError(t, ma.Finish())
	dataNode := builder.Build()

	ptree, treeCid := tree.BuildTestTreeFromData(t, testKeys, testVals)
	treeBuf := new(bytes.Buffer)
	dataBuf := new(bytes.Buffer)
	lsys := ptree.NodeStore().LinkSystem()
	assert.NoError(t, err)

	lnk, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, tree.DefaultLinkProto, dataNode)
	assert.NoError(t, err)
	dataNodeCid := lnk.(cidlink.Link).Cid

	_, err = car2.TraverseV1(context.Background(), lsys, treeCid, selectorparse.CommonSelector_ExploreAllRecursively, treeBuf)
	assert.NoError(t, err)

	_, err = car2.TraverseV1(context.Background(), lsys, dataNodeCid, selectorparse.CommonSelector_ExploreAllRecursively, dataBuf)
	assert.NoError(t, err)

	assert.NoError(t, os.MkdirAll(fmt.Sprintf("./%dRandPairs", count), 0744))

	f, err := os.OpenFile(fmt.Sprintf("./%dRandPairs/data.car", count), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	assert.NoError(t, err)
	_, err = f.Write(dataBuf.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	f, err = os.OpenFile(fmt.Sprintf("./%dRandPairs/tree.car", count), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	assert.NoError(t, err)
	_, err = f.Write(treeBuf.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, f.Close())
}

func TestGenFixtures(t *testing.T) {
	t.SkipNow()
	genFixtures(1, t)
	genFixtures(5, t)
	genFixtures(10000, t)
}

type fixtureSet struct {
	// raw kv pairs
	testKey [][]byte
	testVal []ipld.Node
	// the tree cid that saved in the fixtures(car file)
	treeCid cid.Cid
	// prolly tree load from fixtures(car file)
	ptree *tree.ProllyTree
	// the car size
	carSize int
}

func TestFixtures(t *testing.T) {
	dirs, err := os.ReadDir("./")
	assert.NoError(t, err)
	for _, dir := range dirs {
		fixtureName := dir.Name()
		if !dir.IsDir() {
			continue
		}
		t.Run(fixtureName, func(t *testing.T) {
			fset, err := loadFixture(fixtureName)
			assert.NoError(t, err)
			verifyTree(t, fset)
		})
	}
}

func loadFixture(dir string) (*fixtureSet, error) {
	ctx := context.Background()
	treeSrc, err := os.ReadFile("./" + dir + "/tree.car")
	if err != nil {
		return nil, err
	}
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(ds)
	ns, _ := tree.NewBlockNodeStore(bs, &tree.StoreConfig{CacheSize: 1 << 14})

	ch, err := car.LoadCar(context.Background(), bs, bytes.NewBuffer(treeSrc))
	if err != nil {
		return nil, err
	}
	if len(ch.Roots) != 1 {
		panic("invalid root cid number")
	}

	ptree, err := tree.LoadProllyTreeFromRootCid(ch.Roots[0], ns)
	if err != nil {
		return nil, err
	}
	fset := &fixtureSet{
		treeCid: ch.Roots[0],
		ptree:   ptree,
		carSize: len(treeSrc),
	}

	dataSrc, err := os.ReadFile("./" + dir + "/data.car")
	if err != nil {
		return nil, err
	}
	ch, err = car.LoadCar(context.Background(), bs, bytes.NewBuffer(dataSrc))
	if err != nil {
		return nil, err
	}
	if len(ch.Roots) != 1 {
		panic("invalid root cid number")
	}
	dataNode, err := ns.LinkSystem().Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: ch.Roots[0]}, basicnode.Prototype.Map)
	if err != nil {
		return nil, err
	}
	iter := dataNode.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, err
		}
		kString, err := k.AsString()
		if err != nil {
			return nil, err
		}
		fset.testKey = append(fset.testKey, []byte(kString))
		fset.testVal = append(fset.testVal, v)
	}

	return fset, nil
}

func verifyTree(t *testing.T, fset *fixtureSet) {
	ctx := context.Background()
	ns := tree.TestMemNodeStore()
	// get config
	config, err := fset.ptree.NodeStore().ReadTreeConfig(context.Background(), fset.ptree.Config)
	assert.NoError(t, err)

	// rebuild the tree with same data and config
	framework, err := tree.NewFramework(ctx, ns, config, nil)
	assert.NoError(t, err)

	// the map node can not keep order of pairs, so we need sort them
	muts := tree.NewMutations()
	for i := range fset.testKey {
		assert.NoError(t, muts.AddMutation(&tree.Mutation{
			Key: fset.testKey[i],
			Val: fset.testVal[i],
			Op:  tree.Add,
		}))
	}
	muts.Finish()
	err = framework.AppendFromMutations(ctx, muts)
	assert.NoError(t, err)

	rebuildTree, rTreeCid, err := framework.BuildTree(ctx)
	assert.NoError(t, err)
	// tree cid should equal with the saved tree cid
	assert.Equal(t, rTreeCid, fset.treeCid)

	buf := new(bytes.Buffer)
	size, err := car2.TraverseV1(ctx, rebuildTree.NodeStore().LinkSystem(), rTreeCid, selectorparse.CommonSelector_ExploreAllRecursively, buf)
	assert.NoError(t, err)
	// car size should equal
	assert.Equal(t, int(size), fset.carSize)
}

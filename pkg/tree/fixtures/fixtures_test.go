package fixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	car2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/zeebo/assert"
	"go-ipld-prolly-trees/pkg/tree"
	"os"
	"testing"
)

func genFixtures(count int, t *testing.T) {
	var res [2][][]byte
	testKeys, testVals := tree.RandomTestData(count)
	res[0] = testKeys
	var err error
	for i := range testVals {
		vbytes, err := testVals[i].AsBytes()
		assert.NoError(t, err)
		res[1] = append(res[1], vbytes)
	}

	dataBytes, err := json.Marshal(res)
	assert.NoError(t, err)

	tree, treeCid := tree.BuildTestTreeFromData(t, testKeys, testVals)
	buf := new(bytes.Buffer)
	lsys := tree.NodeStore().LinkSystem()
	assert.NoError(t, err)
	_, err = car2.TraverseV1(context.Background(), lsys, treeCid, selectorparse.CommonSelector_ExploreAllRecursively, buf)
	assert.NoError(t, err)

	assert.NoError(t, os.MkdirAll(fmt.Sprintf("./%dRandPairs", count), 0744))

	f, err := os.OpenFile(fmt.Sprintf("./%dRandPairs/data", count), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	assert.NoError(t, err)

	_, err = f.Write(dataBytes)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	f, err = os.OpenFile(fmt.Sprintf("./%dRandPairs/tree.car", count), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	assert.NoError(t, err)
	_, err = f.Write(buf.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, f.Close())
}

type fixtureSet struct {
	testKey [][]byte
	testVal []ipld.Node
	treeCid cid.Cid
	ptree   *tree.ProllyTree
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
	var data [2][][]byte
	dataSrc, err := os.ReadFile("./" + dir + "/data")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(dataSrc, &data)
	if err != nil {
		return nil, err
	}

	var testVals []ipld.Node
	for i := range data[1] {
		testVals = append(testVals, basicnode.NewBytes(data[1][i]))
	}

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

	return &fixtureSet{
		testKey: data[0],
		testVal: testVals,
		treeCid: ch.Roots[0],
		ptree:   ptree,
		carSize: len(treeSrc),
	}, nil
}

func verifyTree(t *testing.T, fset *fixtureSet) {
	ctx := context.Background()
	ns := tree.TestMemNodeStore()
	config, err := fset.ptree.NodeStore().ReadTreeConfig(context.Background(), fset.ptree.ConfigCid)
	assert.NoError(t, err)

	framework, err := tree.NewFramework(ctx, ns, config, nil)
	assert.NoError(t, err)

	err = framework.AppendBatch(ctx, fset.testKey, fset.testVal)
	assert.NoError(t, err)

	rebuildTree, rTreeCid, err := framework.BuildTree(ctx)
	assert.NoError(t, err)
	assert.Equal(t, rTreeCid, fset.treeCid)

	buf := new(bytes.Buffer)
	size, err := car2.TraverseV1(ctx, rebuildTree.NodeStore().LinkSystem(), rTreeCid, selectorparse.CommonSelector_ExploreAllRecursively, buf)
	assert.NoError(t, err)
	assert.Equal(t, int(size), fset.carSize)
}

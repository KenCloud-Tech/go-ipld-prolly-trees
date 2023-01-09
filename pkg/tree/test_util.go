package tree

import (
	"context"
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/zeebo/assert"
	"math/rand"
	"sort"
	"testing"
)

var testRand = rand.New(rand.NewSource(1))

func RandomTestData(count int) ([][]byte, []ipld.Node) {
	keys := make([][]byte, count)
	vals := make([]ipld.Node, count)

	for i := 0; i < count; i++ {
		key := make([]byte, (testRand.Int63()%30)+15)
		val := make([]byte, (testRand.Int63()%30)+15)
		testRand.Read(key)
		testRand.Read(val)
		keys[i] = key
		vals[i] = basicnode.NewBytes(val)
	}

	// only sorted and removed duplicated keys for test
	dupes := make([]int, 0, count)
	for {
		sort.Slice(keys, func(i, j int) bool {
			return DefaultCompareFunc(keys[i], keys[j]) < 0
		})
		for i := range keys {
			if i == 0 {
				continue
			}
			if DefaultCompareFunc(keys[i], keys[i-1]) == 0 {
				dupes = append(dupes, i)
			}
		}
		if len(dupes) == 0 {
			break
		}

		// replace duplicates and validate again
		for _, d := range dupes {
			key := make([]byte, (testRand.Int63()%30)+15)
			testRand.Read(key)
			keys[d] = key
		}
		dupes = dupes[:0]
	}

	return keys, vals
}

func BuildTestTreeFromData(t *testing.T, keys [][]byte, vals []ipld.Node) *ProllyTree {
	ctx := context.Background()
	ns := TestMemNodeStore()
	cfg := DefaultChunkConfig()
	cfg.Strategy.Suffix.ChunkingFactor = 10
	framwork, err := NewFramework(ctx, ns, cfg, nil)
	assert.NoError(t, err)

	err = framwork.AppendBatch(ctx, keys, vals)
	assert.NoError(t, err)
	tree, _, err := framwork.BuildTree(ctx)
	assert.NoError(t, err)

	return tree
}

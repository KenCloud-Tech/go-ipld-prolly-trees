package tree

import (
	"context"
	"github.com/zeebo/assert"
	"testing"
)

func TestSearchIterator(t *testing.T) {
	ctx := context.Background()
	testKeys, testVals := RandomTestData(50001)

	tree, _ := BuildTestTreeFromData(t, testKeys, testVals)

	start, end := testKeys[5], testKeys[50000]
	iter, err := tree.Search(ctx, start, end)
	assert.NoError(t, err)
	offset := 0
	for !iter.Done() {
		k, v, err := iter.NextPair()
		assert.NoError(t, err)
		assert.Equal(t, k, testKeys[5+offset])
		vBytes, err := v.AsBytes()
		assert.NoError(t, err)
		tBytes, err := testVals[5+offset].AsBytes()
		assert.NoError(t, err)

		assert.Equal(t, vBytes, tBytes)

		offset++
	}
	assert.Equal(t, offset, 49996)
}

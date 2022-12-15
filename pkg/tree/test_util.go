package tree

import (
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"math/rand"
	"sort"
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

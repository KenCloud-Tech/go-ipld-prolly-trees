package tree

import (
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/zeebo/assert"
	"io"
	"testing"
)

func TestMutations(t *testing.T) {
	mut1 := &Mutation{
		Key: []byte("testkey1"),
		Val: basicnode.NewString("abcd"),
		Op:  0,
	}
	mut2 := &Mutation{
		Key: []byte("testkey2"),
		Val: basicnode.NewString("abc5"),
		Op:  0,
	}
	mut3 := &Mutation{
		Key: []byte("testakey3"),
		Val: basicnode.NewString("a12bc"),
		Op:  0,
	}
	mut4 := &Mutation{
		Key: []byte("key4"),
		Val: basicnode.NewString("ab4dsadc"),
		Op:  0,
	}

	ms := NewMutations()
	err := ms.AddMutation(mut1)
	assert.NoError(t, err)
	err = ms.AddMutation(mut2)
	assert.NoError(t, err)
	err = ms.AddMutation(mut3)
	assert.NoError(t, err)
	err = ms.AddMutation(mut4)
	assert.NoError(t, err)

	preMut, err := ms.NextMutation()
	assert.NoError(t, err)
	for {
		mut, err := ms.NextMutation()
		if err != nil {
			break
		}
		assert.True(t, DefaultCompareFunc(preMut.Key, mut.Key) < 0)
		preMut = mut
	}
}

func TestMutationsSorted(t *testing.T) {
	mutations := NewMutations()

	testKeys, testVals := RandomTestData(10000)
	for i := 0; i < len(testKeys); i++ {
		err := mutations.AddMutation(&Mutation{
			Key: testKeys[i],
			Val: testVals[i],
			Op:  Add,
		})
		assert.NoError(t, err)
	}

	preMut, err := mutations.NextMutation()
	assert.NoError(t, err)
	for {
		mut, err := mutations.NextMutation()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		if DefaultCompareFunc(mut.Key, preMut.Key) <= 0 {
			panic("unsorted!")
		}
		preMut = mut
	}
}

package tree

import (
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/zeebo/assert"
	"testing"
)

func TestMutations(t *testing.T) {
	mut1 := &Mutation{
		key: []byte("testkey1"),
		val: basicnode.NewString("abcd"),
		op:  0,
	}
	mut2 := &Mutation{
		key: []byte("testkey2"),
		val: basicnode.NewString("abc5"),
		op:  0,
	}
	mut3 := &Mutation{
		key: []byte("testakey3"),
		val: basicnode.NewString("a12bc"),
		op:  0,
	}
	mut4 := &Mutation{
		key: []byte("key4"),
		val: basicnode.NewString("ab4dsadc"),
		op:  0,
	}

	ms := NewMutations()
	err := ms.addMutation(mut1)
	assert.NoError(t, err)
	err = ms.addMutation(mut2)
	assert.NoError(t, err)
	err = ms.addMutation(mut3)
	assert.NoError(t, err)
	err = ms.addMutation(mut4)
	assert.NoError(t, err)

	preMut, err := ms.NextMutation()
	assert.NoError(t, err)
	for {
		mut, err := ms.NextMutation()
		if err != nil {
			break
		}
		assert.True(t, DefaultCompareFunc(preMut.key, mut.key) < 0)
		preMut = mut
	}
}

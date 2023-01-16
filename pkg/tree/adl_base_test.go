package tree

import (
	"bytes"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/zeebo/assert"
	"io"
	"strings"
	"testing"
)

func testLinkSystem(bs blockstore.Blockstore) *ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link types")
		}
		block, err := bs.Get(lnkCtx.Ctx, asCidLink.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(block.RawData()), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			origBuf := buf.Bytes()

			block, err := blocks.NewBlockWithCid(origBuf, c)
			if err != nil {
				return err
			}
			return bs.Put(lctx.Ctx, block)
		}, nil
	}
	return &lsys
}

func TestCreateAndBuildUse(t *testing.T) {
	prototype := &ProllyTreeADLPrototype{}
	builder := prototype.NewBuilder()
	ptBuilder := builder.(*Builder)
	ptBuilder = ptBuilder.WithLinkSystem(testLinkSystem(blockstore.NewBlockstore(datastore.NewMapDatastore())))
	ma, err := ptBuilder.BeginMap(0)
	assert.NoError(t, err)
	ka := ma.AssembleKey()
	err = ka.AssignBytes([]byte("testkey1"))
	assert.NoError(t, err)
	va := ma.AssembleValue()
	err = va.AssignString("testval1")
	assert.NoError(t, err)
	// close and map Assembler, if assign value, get error
	err = ma.Finish()
	assert.NoError(t, err)
	err = ma.AssembleKey().AssignString("testkey2")
	assert.NoError(t, err)
	err = ma.AssembleValue().AssignFloat(1.234)
	assert.True(t, strings.Contains(err.Error(), "can not add mutation after finished"))

	err = ma.Finish()
	assert.NoError(t, err)

	n := builder.Build()
	vn, err := n.LookupByString("testkey1")
	assert.NoError(t, err)

	res, err := vn.AsString()
	assert.NoError(t, err)

	t.Logf("%s", res)
}

func TestMapIterator(t *testing.T) {
	prototype := &ProllyTreeADLPrototype{}
	builder := prototype.NewBuilder()
	ptBuilder := builder.(*Builder)
	ptBuilder = ptBuilder.WithLinkSystem(testLinkSystem(blockstore.NewBlockstore(datastore.NewMapDatastore())))
	ma, err := ptBuilder.BeginMap(0)
	assert.NoError(t, err)

	count := 10000
	testKeys, testVals := RandomTestData(count)
	for i := 0; i < count; i++ {
		assert.NoError(t, ma.AssembleKey().AssignBytes(testKeys[i]))
		assert.NoError(t, ma.AssembleValue().AssignNode(testVals[i]))
	}
	assert.NoError(t, ma.Finish())
	tree := ptBuilder.Build()
	iter := tree.MapIterator()

	idx := 0
	for !iter.Done() {
		k, v, err := iter.Next()
		assert.NoError(t, err)
		kBytes, err := k.AsBytes()
		assert.NoError(t, err)
		assert.Equal(t, kBytes, testKeys[idx])
		vBytes, err := v.AsBytes()
		assert.NoError(t, err)
		tvBytes, err := testVals[idx].AsBytes()
		assert.NoError(t, err)

		assert.Equal(t, vBytes, tvBytes)
		idx++
	}
	assert.Equal(t, idx, count)

}

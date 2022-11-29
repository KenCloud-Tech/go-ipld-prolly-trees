package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

type ProllyNode struct {
	Config cid.Cid
	Level  int
	Keys   [][]byte
	Links  []cid.Cid
	Values []ipld.Node
}

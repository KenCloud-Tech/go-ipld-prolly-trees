package tree

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type CompareFunc func(left, right []byte) int

type ProllyNode struct {
	IsLeaf bool
	Keys   [][]byte
	Values []ipld.Node
}

func (n *ProllyNode) IsLeafNode() bool {
	return n.IsLeaf
}

// KeyIndex finds the index that the closest but not smaller than the item
func (n *ProllyNode) KeyIndex(item []byte, cp CompareFunc) int {
	length := len(n.Keys)
	l, r := 0, length-1

	for l < r {
		mid := (l + r) / 2
		midKey := n.Keys[mid]
		if cp(midKey, item) == 0 {
			return mid
		} else if cp(midKey, item) > 0 {
			r = mid
		} else {
			l = mid + 1
		}
	}

	return l
}

func (n *ProllyNode) ItemCount() int {
	return len(n.Keys)
}

func (n *ProllyNode) GetIdxKey(i int) []byte {
	return n.Keys[i]
}

func (n *ProllyNode) GetIdxValue(i int) ipld.Node {
	return n.Values[i]
}

func (n *ProllyNode) GetIdxLink(i int) cid.Cid {
	if n.IsLeaf {
		panic("invalid action")
	}
	link, err := n.Values[i].AsLink()
	if err != nil {
		panic(fmt.Errorf("invalid value, expected cidlink, got: %v", n.Values[i]))
	}
	return link.(cidlink.Link).Cid
}

type ProllyRoot struct {
	RootCid   cid.Cid
	ConfigCid cid.Cid
}

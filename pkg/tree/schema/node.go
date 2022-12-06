package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

type CompareFunc func(left, right []byte) int

type ProllyNode struct {
	Config cid.Cid
	Level  int
	Keys   [][]byte
	Links  []cid.Cid
	Values []ipld.Node
}

func (n *ProllyNode) IsLeaf() bool {
	return n.Level == 0
}

func (n *ProllyNode) KeyIndex(item []byte, cp CompareFunc) int {
	length := len(n.Keys)
	l, r := 0, length-1

	// edge condition judge
	if cp(item, n.Keys[r]) >= 0 {
		return r
	}

	for l <= r {
		mid := (l + r) / 2
		midKey := n.Keys[mid]
		if cp(midKey, item) == 0 {
			return mid
		} else if cp(midKey, item) > 0 {
			r = mid - 1
		} else {
			if l == r {
				return l
			}
			if r == l+1 {
				if cp(n.Keys[r], item) <= 0 {
					return r
				}
				return l
			}
			l = mid

			////  avoid loop
			//if l == r || r == l+1 {
			//	if cp(n.Keys[r], item) == 0 {
			//		return r
			//	}
			//	return l
			//}
			//// not mid + 1 because we need the closest key not larger than item, if l = mid + 1, key[l] and key[r] may
			//// be both larger than item
			//l = mid
		}
	}

	panic("invalid")
	return -1
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
	if n.Level == 0 {
		panic("invalid action")
	}
	return n.Links[i]
}

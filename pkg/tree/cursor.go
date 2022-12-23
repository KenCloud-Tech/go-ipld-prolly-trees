package tree

import (
	"bytes"
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	. "go-ipld-prolly-trees/pkg/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

var DefaultCompareFunc CompareFunc = bytes.Compare

type Cursor struct {
	node   *ProllyNode
	idx    int
	ns     types.NodeStore
	parent *Cursor
}

func (cur *Cursor) IsValid() bool {
	l := cur.node.ItemCount()
	if l == 0 {
		return false
	}
	if cur.idx < 0 || cur.idx >= l {
		return false
	}
	return true
}

func (cur *Cursor) IsAtEnd() bool {
	return cur.idx == cur.node.ItemCount()-1
}

func (cur *Cursor) GetLink() cid.Cid {
	if cur.node.IsLeafNode() {
		panic("can not get link from leaf node")
	}
	if !cur.IsValid() {
		panic("get link from invalid cursor")
	}
	return getCidFromIpldNode(cur.node.Values[cur.idx])
}

func CursorAtItem(n *ProllyNode, item []byte, cp CompareFunc, ns types.NodeStore) (*Cursor, error) {
	cur := &Cursor{
		node:   n,
		idx:    n.KeyIndex(item, cp),
		ns:     ns,
		parent: nil,
	}
	for {
		if cur.node.IsLeafNode() {
			break
		}
		childLink := cur.GetLink()
		node, err := ns.ReadNode(context.Background(), childLink)
		if err != nil {
			return nil, err
		}

		parentCur := cur
		cur = &Cursor{
			node:   node,
			idx:    node.KeyIndex(item, cp),
			parent: parentCur,
			ns:     ns,
		}
	}
	return cur, nil
}

func (cur *Cursor) Advance() error {
	l := cur.node.ItemCount()
	if cur.idx < l-1 {
		cur.idx++
		return nil
	}
	if cur.parent == nil {
		cur.idx = l
		return nil
	}
	err := cur.parent.Advance()
	if err != nil {
		return err
	}
	if !cur.parent.IsValid() {
		cur.idx = l
		return nil
	}

	link := cur.parent.GetLink()
	nd, err := cur.ns.ReadNode(context.Background(), link)
	if err != nil {
		return err
	}

	cur.node = nd
	cur.idx = 0
	return nil
}

func (cur *Cursor) GetKey() []byte {
	if !cur.IsValid() {
		panic("get key from invalid cursor")
	}
	return cur.node.GetIdxKey(cur.idx)
}

func (cur *Cursor) GetValue() ipld.Node {
	if !cur.IsValid() {
		panic("get value from invalid cursor")
	}
	return cur.node.GetIdxValue(cur.idx)
}

func (cur *Cursor) Compare(_cur *Cursor) int {
	if cur == nil && _cur == nil {
		return 0
	} else if cur == nil || _cur == nil {
		panic("can not compare two cursors with different height")
	}
	diff := cur.idx - _cur.idx
	if pdiff := cur.parent.Compare(_cur.parent); pdiff != 0 {
		diff = pdiff
	}
	return diff
}

func (cur *Cursor) Equal(_cur *Cursor) bool {
	if cur.Compare(_cur) == 0 {
		return true
	}
	return false
}

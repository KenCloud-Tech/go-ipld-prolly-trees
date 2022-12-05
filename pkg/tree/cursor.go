package tree

import (
	"bytes"
	"context"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	. "go-ipld-prolly-trees/pkg/tree/schema"
	"go-ipld-prolly-trees/pkg/tree/types"
)

var DefaultCompareFunc CompareFunc = bytes.Compare

type Cursor struct {
	node         *ProllyNode
	idx          int
	ns           types.NodeStore
	parentCursor *Cursor
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

func (cur *Cursor) GetLink() cid.Cid {
	if cur.node.IsLeaf() {
		panic("can not get link from leaf node")
	}
	if !cur.IsValid() {
		panic("get link from invalid cursor")
	}
	return cur.node.Links[cur.idx]
}

func CursorAtItem(n *ProllyNode, item []byte, cp CompareFunc, ns types.NodeStore) (*Cursor, error) {
	cur := &Cursor{
		node:         n,
		idx:          n.KeyIndex(item, cp),
		parentCursor: nil,
	}
	for {
		if cur.node.IsLeaf() {
			break
		}
		childLink := cur.GetLink()
		node, err := ns.ReadNode(context.Background(), childLink)
		if err != nil {
			return nil, err
		}

		parentCur := cur
		cur = &Cursor{
			node:         node,
			idx:          node.KeyIndex(item, cp),
			parentCursor: parentCur,
		}
	}
	return cur, nil
}

func (cur *Cursor) AdvanceCursor() error {
	l := cur.node.ItemCount()
	if cur.idx < l-1 {
		cur.idx++
		return nil
	}
	if cur.parentCursor == nil {
		cur.idx = l
		return nil
	}
	err := cur.parentCursor.AdvanceCursor()
	if err != nil {
		return err
	}
	if !cur.parentCursor.IsValid() {
		cur.idx = l
		return nil
	}

	link := cur.parentCursor.GetLink()
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
	if !cur.node.IsLeaf() {
		panic("get value from branch node")
	}
	if !cur.IsValid() {
		panic("get value from invalid cursor")
	}
	return cur.node.GetIdxValue(cur.idx)
}

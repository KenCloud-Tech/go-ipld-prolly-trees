package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
)

var DefaultCompareFunc CompareFunc = bytes.Compare

type Cursor struct {
	node   *ProllyNode
	idx    int
	ns     NodeStore
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

func (cur *Cursor) isAtStart() bool {
	return cur.idx == 0
}

func (cur *Cursor) IsBiggerThanTheNode(key []byte) bool {
	// only call the function while cur at tail
	if !cur.IsAtEnd() {
		return false
	}
	return DefaultCompareFunc(key, cur.GetKey()) > 0
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

func CursorAtItem(root *ProllyNode, item []byte, cp CompareFunc, ns NodeStore) (*Cursor, error) {
	cur := &Cursor{
		node:   root,
		idx:    root.KeyIndex(item, cp),
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
		panic("get Key from invalid cursor")
	}
	return cur.node.GetIdxKey(cur.idx)
}

func (cur *Cursor) GetValue() ipld.Node {
	if !cur.IsValid() {
		panic("get value from invalid cursor")
	}
	return cur.node.GetIdxValue(cur.idx)
}

func (cur *Cursor) GetTreeCount() uint32 {
	return cur.node.GetIdxTreeCount(cur.idx)
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

func (cur *Cursor) Equal(other *Cursor) bool {
	if cur.Compare(other) == 0 {
		return true
	}
	return false
}

func (cur *Cursor) copy(other *Cursor) {
	cur.node = other.node
	cur.idx = other.idx
	// is it must?
	cur.ns = other.ns

	if cur.parent != nil {
		if other.parent == nil {
			panic("can not copy from cursor with different height")
		}
		cur.parent.copy(other.parent)
	} else {
		if other.parent != nil {
			panic("can not copy from cursor with different height")
		}
	}
}

func (cur *Cursor) SkipCommon(other *Cursor) error {
	var err error
	if !cur.equalKeyValuePair(other) {
		return fmt.Errorf("must start from same key/value pair")
	}

	// if parents exist and equal, try skip in higher level
	if cur.parent != nil && other.parent != nil && cur.parent.equalKeyValuePair(other.parent) {
		err = cur.parent.SkipCommon(other.parent)
		if err != nil {
			return err
		}

		// the parents have skipped the common parts, now set the children cursor at right location

		// set cursor in the end(invalid) if parent is invalid
		if !cur.parent.IsValid() {
			link := getCidFromIpldNode(cur.parent.node.GetIdxValue(cur.parent.node.ItemCount() - 1))
			nd, err := cur.ns.ReadNode(context.Background(), link)
			if err != nil {
				return err
			}
			cur.node = nd
			cur.idx = cur.node.ItemCount()
		} else {
			link := cur.parent.GetLink()
			nd, err := cur.ns.ReadNode(context.Background(), link)
			if err != nil {
				return err
			}
			cur.node = nd
			cur.idx = 0
		}

		// the same with beyond
		if !other.parent.IsValid() {
			link := getCidFromIpldNode(other.parent.node.GetIdxValue(other.parent.node.ItemCount() - 1))
			nd, err := other.ns.ReadNode(context.Background(), link)
			if err != nil {
				return err
			}
			other.node = nd
			other.idx = other.node.ItemCount()
		} else {
			link := other.parent.GetLink()
			nd, err := other.ns.ReadNode(context.Background(), link)
			if err != nil {
				return err
			}
			other.node = nd
			other.idx = 0
		}
	} else {
		// can not skip in higher level, advance together until:
		// 1. either or both cursors invalid, return
		// 2. differ , return
		// 3. both arrive the start of new nodes, try skip in higher level, not return
		for {
			err = cur.Advance()
			if err != nil {
				return err
			}
			err = other.Advance()
			if err != nil {
				return err
			}
			// either cursor arrives the end
			if !cur.IsValid() || !other.IsValid() {
				return nil
			}
			// cursors differ
			if !cur.equalKeyValuePair(other) {
				return nil
			}
			// try skip in higher level
			if cur.isAtStart() && other.isAtStart() {
				err = cur.SkipCommon(other)
				if err != nil {
					return err
				}
			}
		}
	}

	panic("should not arrive here")
}

func (cur *Cursor) equalKeyValuePair(other *Cursor) bool {
	if !bytes.Equal(cur.GetKey(), other.GetKey()) {
		return false
	}
	if !bytes.Equal(EncodeNode(cur.GetValue()), EncodeNode(other.GetValue())) {
		return false
	}
	return true
}

func EncodeNode(n ipld.Node) []byte {
	buf := bytes.Buffer{}
	if err := dagcbor.Encode(n, &buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (cur *Cursor) GetIndex() int {
	return cur.idx
}

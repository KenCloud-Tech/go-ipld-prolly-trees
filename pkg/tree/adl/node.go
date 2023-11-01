package adl

import (
	"context"
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/node/mixins"
	"github.com/kenlabs/go-ipld-prolly-trees/pkg/tree"
)

var _ datamodel.Node = &Node{}
var _ adl.ADL = &Node{}

type Node struct {
	*tree.ProllyTree
}

func (n *Node) WithLinkSystem(lsys *ipld.LinkSystem) *Node {
	ns := tree.NewLinkSystemNodeStore(lsys)
	err := n.LoadProllyTreeFromRootNode(ns)
	if err != nil {
		panic(err)
	}

	return n
}

func (n *Node) Substrate() datamodel.Node {
	return bindnode.Wrap(&n.ProllyRoot, tree.ProllyTreePrototype.Type())
}

func (n *Node) Kind() datamodel.Kind {
	return ipld.Kind_Map
}

func (n *Node) LookupByString(key string) (datamodel.Node, error) {
	return n.Get([]byte(key))
}

func (n *Node) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	if kbytes, err := key.AsBytes(); err == nil {
		return n.Get(kbytes)
	}
	if kstring, err := key.AsString(); err == nil {
		return n.LookupByString(kstring)
	}
	return nil, fmt.Errorf("invalid ipld.node as key:%v", key)
}

func (n *Node) LookupByIndex(int64) (datamodel.Node, error) {
	return mixins.Map{TypeName: "ProllyTree"}.LookupByIndex(0)
}

func (n *Node) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return n.LookupByString(seg.String())
}

func (n *Node) MapIterator() datamodel.MapIterator {
	start, err := n.FirstKey()
	if err != nil {
		panic(err)
	}
	iter, err := n.Search(context.Background(), start, nil)
	if err != nil {
		panic(err)
	}
	return iter
}

func (n *Node) ListIterator() datamodel.ListIterator {
	return mixins.Map{TypeName: "ProllyTree"}.ListIterator()
}

func (n *Node) Length() int64 {
	return int64(n.TreeCount())
}

func (n *Node) IsAbsent() bool {
	return mixins.Map{TypeName: "ProllyTree"}.IsAbsent()
}

func (n *Node) IsNull() bool {
	return mixins.Map{TypeName: "ProllyTree"}.IsNull()
}

func (n *Node) AsBool() (bool, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsBool()
}

func (n *Node) AsInt() (int64, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsInt()
}

func (n *Node) AsFloat() (float64, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsFloat()
}

func (n *Node) AsString() (string, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsString()
}

func (n *Node) AsBytes() ([]byte, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsBytes()
}

func (n *Node) AsLink() (datamodel.Link, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsLink()
}

func (n *Node) Prototype() datamodel.NodePrototype {
	return tree.ProllyTreePrototype
}

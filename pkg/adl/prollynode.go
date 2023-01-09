package adl

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
	"go-ipld-prolly-trees/pkg/schema"
	"go-ipld-prolly-trees/pkg/tree"
	nodestore "go-ipld-prolly-trees/pkg/tree/node_store"
)

var _ datamodel.Node = &Node{}
var _ adl.ADL = &Node{}

type Node struct {
	*schema.ProllyTreeNode
	tree *tree.ProllyTree
}

func (n Node) WithLinkSystem(lsys *ipld.LinkSystem) *Node {
	n.tree.Ns = nodestore.NewLinkSystemNodeStore(lsys)
	if n.tree == nil {
		var err error
		n.tree, err = tree.LoadProllyTreeFromRootNode(n.ProllyTreeNode, n.tree.Ns)
		if err != nil {
			panic(err)
		}
	}
	return &n
}

func (n Node) Substrate() datamodel.Node {
	//TODO implement me
	panic("implement me")
}

func (n Node) Kind() datamodel.Kind {
	return ipld.Kind_Map
}

func (n Node) LookupByString(key string) (datamodel.Node, error) {
	return n.tree.Get([]byte(key))
}

func (n Node) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	if kbytes, err := key.AsBytes(); err == nil {
		return n.tree.Get(kbytes)
	}
	if kstring, err := key.AsString(); err == nil {
		return n.LookupByString(kstring)
	}
	return nil, fmt.Errorf("invalid ipld.node as key:%v", key)
}

func (n Node) LookupByIndex(idx int64) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return n.LookupByString(seg.String())
}

func (n Node) MapIterator() datamodel.MapIterator {
	//TODO implement me
	panic("implement me")
}

func (n Node) ListIterator() datamodel.ListIterator {
	//TODO implement me
	panic("implement me")
}

func (n Node) Length() int64 {
	//TODO implement me
	panic("implement me")
}

func (n Node) IsAbsent() bool {
	//TODO implement me
	panic("implement me")
}

func (n Node) IsNull() bool {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsBool() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsInt() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsFloat() (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsString() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsBytes() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) AsLink() (datamodel.Link, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) Prototype() datamodel.NodePrototype {
	return schema.ProllyTreePrototype
}

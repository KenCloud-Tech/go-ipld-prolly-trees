package adl

import (
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
)

var _ datamodel.Node = &Node{}
var _ adl.ADL = &Node{}

type Node struct {
}

func (n Node) Substrate() datamodel.Node {
	//TODO implement me
	panic("implement me")
}

func (n Node) Kind() datamodel.Kind {
	//TODO implement me
	panic("implement me")
}

func (n Node) LookupByString(key string) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) LookupByIndex(idx int64) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (n Node) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

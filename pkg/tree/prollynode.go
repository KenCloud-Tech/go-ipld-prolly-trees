package tree

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
)

var _ datamodel.Node = &ProllyTree{}
var _ adl.ADL = &ProllyTree{}

func (pt *ProllyTree) WithLinkSystem(lsys *ipld.LinkSystem) *ProllyTree {
	pt.Ns = NewLinkSystemNodeStore(lsys)
	err := pt.loadProllyTreeFromRootNode(pt.Ns)
	if err != nil {
		panic(err)
	}
	//if pt.tree == nil {
	//	var err error
	//	pt.tree, err = LoadProllyTreeFromRootNode(pt.ProllyRoot, pt.tree.Ns)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	return pt
}

func (pt *ProllyTree) Substrate() datamodel.Node {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) Kind() datamodel.Kind {
	return ipld.Kind_Map
}

func (pt *ProllyTree) LookupByString(key string) (datamodel.Node, error) {
	return pt.Get([]byte(key))
}

func (pt *ProllyTree) LookupByNode(key datamodel.Node) (datamodel.Node, error) {
	if kbytes, err := key.AsBytes(); err == nil {
		return pt.Get(kbytes)
	}
	if kstring, err := key.AsString(); err == nil {
		return pt.LookupByString(kstring)
	}
	return nil, fmt.Errorf("invalid ipld.node as key:%v", key)
}

func (pt *ProllyTree) LookupByIndex(idx int64) (datamodel.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return pt.LookupByString(seg.String())
}

func (pt *ProllyTree) MapIterator() datamodel.MapIterator {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) ListIterator() datamodel.ListIterator {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) Length() int64 {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) IsAbsent() bool {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) IsNull() bool {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsBool() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsInt() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsFloat() (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsString() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsBytes() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) AsLink() (datamodel.Link, error) {
	//TODO implement me
	panic("implement me")
}

func (pt *ProllyTree) Prototype() datamodel.NodePrototype {
	return ProllyTreePrototype
}

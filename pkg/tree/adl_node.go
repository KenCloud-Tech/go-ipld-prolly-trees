package tree

import (
	"context"
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/adl"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/node/mixins"
)

var _ datamodel.Node = &ProllyTree{}
var _ adl.ADL = &ProllyTree{}

func (pt *ProllyTree) WithLinkSystem(lsys *ipld.LinkSystem) *ProllyTree {
	pt.Ns = NewLinkSystemNodeStore(lsys)
	err := pt.loadProllyTreeFromRootNode(pt.Ns)
	if err != nil {
		panic(err)
	}

	return pt
}

func (pt *ProllyTree) Substrate() datamodel.Node {
	return bindnode.Wrap(&pt.ProllyRoot, ProllyTreePrototype.Type())
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

func (pt *ProllyTree) LookupByIndex(int64) (datamodel.Node, error) {
	return mixins.Map{TypeName: "ProllyTree"}.LookupByIndex(0)
}

func (pt *ProllyTree) LookupBySegment(seg datamodel.PathSegment) (datamodel.Node, error) {
	return pt.LookupByString(seg.String())
}

func (pt *ProllyTree) MapIterator() datamodel.MapIterator {
	start, err := pt.firstKey()
	if err != nil {
		panic(err)
	}
	iter, err := pt.Search(context.Background(), start, nil)
	if err != nil {
		panic(err)
	}
	return iter
}

func (pt *ProllyTree) ListIterator() datamodel.ListIterator {
	return mixins.Map{TypeName: "ProllyTree"}.ListIterator()
}

func (pt *ProllyTree) Length() int64 {
	panic("implement me")
}

func (pt *ProllyTree) IsAbsent() bool {
	return mixins.Map{TypeName: "ProllyTree"}.IsAbsent()
}

func (pt *ProllyTree) IsNull() bool {
	return mixins.Map{TypeName: "ProllyTree"}.IsNull()
}

func (pt *ProllyTree) AsBool() (bool, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsBool()
}

func (pt *ProllyTree) AsInt() (int64, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsInt()
}

func (pt *ProllyTree) AsFloat() (float64, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsFloat()
}

func (pt *ProllyTree) AsString() (string, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsString()
}

func (pt *ProllyTree) AsBytes() ([]byte, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsBytes()
}

func (pt *ProllyTree) AsLink() (datamodel.Link, error) {
	return mixins.Map{TypeName: "ProllyTree"}.AsLink()
}

func (pt *ProllyTree) Prototype() datamodel.NodePrototype {
	return ProllyTreePrototype
}

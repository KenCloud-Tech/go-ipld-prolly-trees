package adl

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/mixins"
	"go-ipld-prolly-trees/pkg/schema"
)

var _ ipld.NodePrototype = &ProllyTreePrototype{}

type ProllyTreePrototype struct {
}

func (p ProllyTreePrototype) NewBuilder() datamodel.NodeBuilder {
	cfg := schema.DefaultChunkConfig()
	return &Builder{
		cfg: cfg,
	}
}

var _ ipld.NodeBuilder = &Builder{}
var _ ipld.NodeAssembler = &Builder{}

type Builder struct {
	cfg  *schema.TreeConfig
	lsys *ipld.LinkSystem
}

func (b *Builder) WithLinkSystem(lsys *ipld.LinkSystem) *Builder {
	if lsys == nil {
		panic("nil linksystem")
	}
	b.lsys = lsys
	return b
}

func (b *Builder) WithConfig(cfg *schema.TreeConfig) *Builder {
	if cfg == nil {
		panic("nil config")
	}
	b.cfg = cfg
	return b
}

func (b *Builder) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	//TODO implement me
	return &TreeAssembler{}, nil
}

func (b *Builder) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.BeginList(0)
}

func (b *Builder) AssignNull() error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignNull()
}

func (b *Builder) AssignBool(bool bool) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignBool(bool)
}

func (b *Builder) AssignInt(i int64) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignInt(i)
}

func (b *Builder) AssignFloat(f float64) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignFloat(f)
}

func (b *Builder) AssignString(s string) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignString(s)
}

func (b *Builder) AssignBytes(bytes []byte) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignBytes(bytes)
}

func (b *Builder) AssignLink(link datamodel.Link) error {
	return mixins.MapAssembler{TypeName: "ProllyTreeADL.Node"}.AssignLink(link)
}

func (b Builder) AssignNode(node datamodel.Node) error {
	//TODO implement me
	panic("implement me")
}

func (b Builder) Prototype() datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
}

func (b Builder) Build() datamodel.Node {
	//TODO implement me
	panic("implement me")
}

func (b Builder) Reset() {
	//TODO implement me
	panic("implement me")
}

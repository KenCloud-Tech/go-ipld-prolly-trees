package tree

import (
	"context"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/mixins"
)

var _ ipld.NodePrototype = &ProllyTreeADLPrototype{}

type ProllyTreeADLPrototype struct {
}

func (p ProllyTreeADLPrototype) NewBuilder() datamodel.NodeBuilder {
	cfg := DefaultChunkConfig()
	return &Builder{
		cfg: cfg,
	}
}

var _ ipld.NodeBuilder = &Builder{}
var _ ipld.NodeAssembler = &Builder{}

type Builder struct {
	cfg  *TreeConfig
	ns   NodeStore
	fw   *Framework
	muts *Mutations
}

func (b *Builder) WithLinkSystem(lsys *ipld.LinkSystem) *Builder {
	if lsys == nil {
		panic("nil linksystem")
	}
	b.ns = NewLinkSystemNodeStore(lsys)
	return b
}

func (b *Builder) WithConfig(cfg *TreeConfig) *Builder {
	if cfg == nil {
		panic("nil config")
	}
	b.cfg = cfg
	return b
}

func (b *Builder) BeginMap(_ int64) (datamodel.MapAssembler, error) {
	var err error
	b.fw, err = NewFramework(context.Background(), b.ns, b.cfg, nil)
	if err != nil {
		return nil, err
	}
	b.muts = NewMutations()
	return &TreeAssembler{muts: b.muts}, nil
}

func (b *Builder) BeginList(int64) (datamodel.ListAssembler, error) {
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

func (b *Builder) AssignNode(node datamodel.Node) error {
	panic("implement me")
}

func (b *Builder) Prototype() datamodel.NodePrototype {
	return ProllyTreeADLPrototype{}
}

func (b *Builder) Build() datamodel.Node {
	err := b.fw.AppendFromMutations(context.Background(), b.muts)
	if err != nil {
		panic(err)
	}
	prollyTree, _, err := b.fw.BuildTree(context.Background())
	if err != nil {
		panic(err)
	}
	return prollyTree
}

func (b *Builder) Reset() {
	b.ns = nil
	b.fw = nil
	b.muts = nil
}

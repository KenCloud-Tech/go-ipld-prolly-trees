package tree

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/mixins"
)

var _ ipld.MapAssembler = &TreeAssembler{}

type TreeAssembler struct {
	muts *Mutations
	key  []byte
}

func (t *TreeAssembler) AssembleKey() datamodel.NodeAssembler {
	return &keyAssembler{ta: t}
}

func (t *TreeAssembler) AssembleValue() datamodel.NodeAssembler {
	return &valueAssembler{ta: t}
}

func (t *TreeAssembler) AssembleEntry(k string) (datamodel.NodeAssembler, error) {
	return nil, fmt.Errorf("not supported")
}

func (t *TreeAssembler) Finish() error {
	t.muts.Finish()
	return nil
}

func (t *TreeAssembler) KeyPrototype() datamodel.NodePrototype {
	return basicnode.Prototype.Bytes
}

func (t *TreeAssembler) ValuePrototype(k string) datamodel.NodePrototype {
	return basicnode.Prototype.Any
}

var _ ipld.NodeAssembler = &keyAssembler{}

type keyAssembler struct {
	ta *TreeAssembler
}

func (k *keyAssembler) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginMap(sizeHint)
}

func (k *keyAssembler) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return mixins.BytesAssembler{TypeName: "bytes"}.BeginList(sizeHint)

}

func (k *keyAssembler) AssignNull() error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignNull()
}

func (k *keyAssembler) AssignBool(b bool) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignBool(b)
}

func (k *keyAssembler) AssignInt(i int64) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignInt(i)
}

func (k *keyAssembler) AssignFloat(f float64) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignFloat(f)
}

func (k *keyAssembler) AssignString(s string) error {
	k.ta.key = []byte(s)
	return nil
}

func (k *keyAssembler) AssignBytes(bytes []byte) error {
	k.ta.key = bytes
	return nil
}

func (k *keyAssembler) AssignLink(link datamodel.Link) error {
	return mixins.BytesAssembler{TypeName: "bytes"}.AssignLink(link)
}

func (k *keyAssembler) AssignNode(node datamodel.Node) error {
	bytes, err := node.AsBytes()
	if err == nil {
		return k.AssignBytes(bytes)
	}
	str, err := node.AsString()
	if err == nil {
		return k.AssignString(str)
	}

	return fmt.Errorf("unsupported key type: %s", node.Kind().String())
}

func (k *keyAssembler) Prototype() datamodel.NodePrototype {
	return basicnode.Prototype.Bytes
}

var _ ipld.NodeAssembler = &valueAssembler{}

type valueAssembler struct {
	ta *TreeAssembler
}

func (v *valueAssembler) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	return nil, fmt.Errorf("unsupported value type")

}

func (v *valueAssembler) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	return nil, fmt.Errorf("unsupported value type")

}

func (v *valueAssembler) AssignNull() error {
	return fmt.Errorf("unsupported value type")
}

func (v *valueAssembler) AssignBool(b bool) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignBool(b); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignInt(i int64) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignInt(i); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignFloat(f float64) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignFloat(f); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignString(s string) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignString(s); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignBytes(bytes []byte) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignBytes(bytes); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignLink(link datamodel.Link) error {
	builder := basicnode.Prototype.Any.NewBuilder()
	if err := builder.AssignLink(link); err != nil {
		return err
	}
	return v.AssignNode(builder.Build())
}

func (v *valueAssembler) AssignNode(node datamodel.Node) error {
	key := v.ta.key
	if key == nil {
		return fmt.Errorf("must assign key first")
	}
	v.ta.key = nil
	return v.ta.muts.AddMutation(&Mutation{
		Key: key,
		Val: node,
		Op:  Add,
	})
}

func (v *valueAssembler) Prototype() datamodel.NodePrototype {
	return basicnode.Prototype.Any
}

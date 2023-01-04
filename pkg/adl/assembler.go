package adl

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"go-ipld-prolly-trees/pkg/tree"
)

var _ ipld.MapAssembler = &TreeAssembler{}

type TreeAssembler struct {
	muts *tree.Mutations
	key  []byte
}

func (t *TreeAssembler) AssembleKey() datamodel.NodeAssembler {
	//TODO implement me
	return &keyAssembler{ta: t}
}

func (t *TreeAssembler) AssembleValue() datamodel.NodeAssembler {
	//TODO implement me
	return &valueAssembler{ta: t}
}

func (t *TreeAssembler) AssembleEntry(k string) (datamodel.NodeAssembler, error) {
	//TODO implement me
	panic("implement me")
}

func (t *TreeAssembler) Finish() error {
	//TODO implement me
	panic("implement me")
}

func (t *TreeAssembler) KeyPrototype() datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
}

func (t *TreeAssembler) ValuePrototype(k string) datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
}

var _ ipld.NodeAssembler = &keyAssembler{}

type keyAssembler struct {
	ta *TreeAssembler
}

func (k *keyAssembler) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignNull() error {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignBool(b bool) error {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignInt(i int64) error {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignFloat(f float64) error {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignNode(node datamodel.Node) error {
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) Prototype() datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
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
	return v.ta.muts.AddMutation(&tree.Mutation{
		Key: key,
		Val: node,
		Op:  tree.Add,
	})
}

func (v *valueAssembler) Prototype() datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
}

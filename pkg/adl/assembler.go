package adl

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
)

var _ ipld.MapAssembler = &TreeAssembler{}

type TreeAssembler struct {
}

func (t *TreeAssembler) AssembleKey() datamodel.NodeAssembler {
	//TODO implement me
	return &keyAssembler{}
}

func (t *TreeAssembler) AssembleValue() datamodel.NodeAssembler {
	//TODO implement me
	return &valueAssembler{}
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
	//TODO implement me
	panic("implement me")
}

func (k *keyAssembler) AssignBytes(bytes []byte) error {
	//TODO implement me
	panic("implement me")
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
}

func (v *valueAssembler) BeginMap(sizeHint int64) (datamodel.MapAssembler, error) {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) BeginList(sizeHint int64) (datamodel.ListAssembler, error) {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignNull() error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignBool(b bool) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignInt(i int64) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignFloat(f float64) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignString(s string) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignBytes(bytes []byte) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignLink(link datamodel.Link) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) AssignNode(node datamodel.Node) error {
	//TODO implement me
	panic("implement me")
}

func (v *valueAssembler) Prototype() datamodel.NodePrototype {
	//TODO implement me
	panic("implement me")
}

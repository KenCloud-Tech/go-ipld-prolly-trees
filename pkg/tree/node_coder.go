package tree

import (
	"bytes"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/multicodec"
)

type NodeCoder struct {
	codecRegistry *multicodec.Registry
	encoder       codec.Encoder
}

func NewNodeCoder() *NodeCoder {
	nc := &NodeCoder{}
	nc.codecRegistry = &multicodec.DefaultRegistry
	return nc
}

func (nc *NodeCoder) InitEncoder(codec uint64) error {
	encoder, err := nc.codecRegistry.LookupEncoder(codec)
	if err != nil {
		return err
	}
	nc.encoder = encoder
	return nil
}

func (nc *NodeCoder) LoadEncoder(indicator uint64, encodeFunc codec.Encoder) error {
	nc.codecRegistry.RegisterEncoder(indicator, encodeFunc)
	return nil
}

func (nc *NodeCoder) EncodeNode(node ipld.Node) ([]byte, error) {
	if nc.encoder == nil {
		panic("init encoder before using")
	}
	buf := new(bytes.Buffer)
	err := nc.encoder(node, buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

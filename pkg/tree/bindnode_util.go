package tree

import (
	"errors"
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
)

func (n *ProllyNode) ToNode() (nd ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	nd = bindnode.Wrap(n, ProllyNodePrototype.Type()).Representation()
	return
}

func UnwrapProllyNode(node ipld.Node) (*ProllyNode, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode will not work.
	// Here we defensively check the prototype and wrap if needed, since:
	//   - linksystem in sti is passed into other libraries, like go-legs, and
	//   - for whatever reason clients of this package may load nodes using Prototype.Any.
	//
	// The code in this repo, however should load nodes with appropriate prototype and never trigger
	// this if statement.
	if node.Prototype() != ProllyNodePrototype {
		pnBuilder := ProllyNodePrototype.NewBuilder()
		err := pnBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = pnBuilder.Build()
	}

	nd, ok := bindnode.Unwrap(node).(*ProllyNode)
	if !ok || nd == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.ProllyNode")
	}
	return nd, nil
}

func (pt *ProllyTree) ToNode() (nd ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	nd = bindnode.Wrap(&pt.ProllyRoot, ProllyTreePrototype.Type()).Representation()
	return
}

func UnwrapProllyTree(node ipld.Node) (*ProllyTree, error) {
	if node.Prototype() != ProllyTreePrototype {
		prBuilder := ProllyTreePrototype.NewBuilder()
		err := prBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = prBuilder.Build()
	}

	nd, ok := bindnode.Unwrap(node).(*ProllyRoot)
	if !ok || nd == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.ProllyRoot")
	}
	return &ProllyTree{ProllyRoot: *nd}, nil
}

func toError(r interface{}) error {
	switch x := r.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return fmt.Errorf("unknown panic: %v", r)
	}
}

func (cfg *TreeConfig) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(cfg, ChunkConfigPrototype.Type()).Representation()
	return
}

func UnwrapChunkConfig(node ipld.Node) (*TreeConfig, error) {
	if node.Prototype() != ChunkConfigPrototype {
		cfgBuilder := ChunkConfigPrototype.NewBuilder()
		err := cfgBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = cfgBuilder.Build()
	}

	cfg, ok := bindnode.Unwrap(node).(*TreeConfig)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.TreeConfig")
	}
	return cfg, nil
}

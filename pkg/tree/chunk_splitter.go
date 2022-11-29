package tree

import "go-ipld-prolly-trees/pkg/tree/schema"

type Splitter interface {
	ShouldCreateBoundary(node *schema.ProllyNode, key []byte) bool
}

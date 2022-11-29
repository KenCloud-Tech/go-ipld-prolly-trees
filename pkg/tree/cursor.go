package tree

import "go-ipld-prolly-trees/pkg/tree/schema"

type Cursor struct {
	node         *schema.ProllyNode
	idx          int
	parentCursor *Cursor
}

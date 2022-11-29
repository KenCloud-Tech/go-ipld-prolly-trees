package tree

import (
	"github.com/ipld/go-ipld-prime"
	"go-ipld-prolly-trees/pkg/tree/schema"
)

type nodeBuilder struct {
	keys   [][]byte
	values []ipld.Node
	level  int
}

type LevelBuilder struct {
	chunkConfig   *schema.ChunkConfig
	level         int
	cursor        *Cursor
	nodeBuffer    *nodeBuilder
	parentBuilder *LevelBuilder
	done          bool
}

type Framework struct {
	done     bool
	builders []*LevelBuilder
}

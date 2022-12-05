package tree

import (
	"fmt"
	"github.com/zeebo/xxh3"
	. "go-ipld-prolly-trees/pkg/tree/schema"
	"math"
)

type Splitter interface {
	// IsBoundary returns whether boundary generated at current location
	IsBoundary() bool
	Append(key, val []byte) error
	Reset()
}

type WeibullSplitter struct {
	isBoundary     bool
	totalItemsSize int
	config         *ChunkConfig
}

func (ws *WeibullSplitter) IsBoundary() bool {
	return ws.isBoundary
}

func (ws *WeibullSplitter) Append(key, val []byte) error {
	// can't append until reset splitter after boundary generated
	if ws.isBoundary {
		return fmt.Errorf("boundary generated but not reset")
	}
	// the function may be configurable
	hash := xxh3.Hash(append(key, val...))

	itemSize := len(key) + len(val)
	ws.totalItemsSize += itemSize

	// can not split
	if ws.totalItemsSize < ws.config.MinChunkSize {
		return nil
	}
	// must split
	if ws.totalItemsSize > ws.config.MaxChunkSize {
		ws.isBoundary = true
		return nil
	}

	start := weibullCDF(ws.totalItemsSize-itemSize, ws.config.Strategy.Weilbull.K, ws.config.Strategy.Weilbull.L)
	end := weibullCDF(ws.totalItemsSize, ws.config.Strategy.Weilbull.K, ws.config.Strategy.Weilbull.L)
	p := float64(hash) / math.MaxUint32
	d := 1 - start
	target := (end - start) / d
	if p < target {
		ws.isBoundary = true
	}

	return nil
}

func (ws *WeibullSplitter) Reset() {
	ws.totalItemsSize = 0
	ws.isBoundary = false
}

// -exp(-pow(x/L),K)
func weibullCDF(x int, K, L float64) float64 {
	return -math.Exp(-math.Pow(float64(x)/L, K))
}

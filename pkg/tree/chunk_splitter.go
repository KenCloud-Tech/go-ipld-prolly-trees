package tree

import (
	"fmt"
	"github.com/zeebo/xxh3"
	. "go-ipld-prolly-trees/pkg/tree/schema"
)

type Splitter interface {
	// IsBoundary returns whether boundary generated at current location
	IsBoundary() bool
	Append(key, val []byte) error
	Reset()
}

var _ Splitter = &PrefixSplitter{}

type PrefixSplitter struct {
	isBoundary       bool
	totalBytesSize   int
	totalPairsNumber int
	pattern          uint64
	config           *ChunkConfig
}

func NewSplitterFromConfig(config *ChunkConfig) Splitter {
	var splitter Splitter
	switch config.StrategyType {
	case PrefixThreshold:
		splitter = &PrefixSplitter{
			config:  config,
			pattern: uint64(1<<config.Strategy.Prefix.ChunkingFactor - 1),
		}
	default:
		panic(fmt.Errorf("unsupported chunk strategy: %v", config.StrategyType))
	}
	return splitter
}

func (p *PrefixSplitter) IsBoundary() bool {
	return p.isBoundary
}

func (p *PrefixSplitter) Append(key, val []byte) error {
	// can't append until reset splitter after boundary generated
	if p.isBoundary {
		return fmt.Errorf("boundary generated but not reset")
	}
	input := append(key, val...)
	inputSize := len(input)

	p.totalBytesSize += inputSize
	p.totalPairsNumber += 1

	// MaxPairsInNode's priority is higher than MinNodeSize
	if p.totalPairsNumber >= p.config.MaxPairsInNode {
		p.isBoundary = true
		return nil
	}

	if p.totalBytesSize < p.config.MinNodeSize {
		return nil
	}

	if p.totalBytesSize >= p.config.MaxNodeSize {
		p.isBoundary = true
		return nil
	}

	h := xxh3.Hash(input)

	if h&p.pattern == 0 {
		p.isBoundary = true
	}
	return nil
}

func (p *PrefixSplitter) Reset() {
	p.isBoundary = false
}

//type WeibullSplitter struct {
//	isBoundary     bool
//	totalItemsSize int
//	config         *ChunkConfig
//}
//
//func (ws *WeibullSplitter) IsBoundary() bool {
//	return ws.isBoundary
//}
//
//func (ws *WeibullSplitter) Append(key, val []byte) error {
//	// can't append until reset splitter after boundary generated
//	if ws.isBoundary {
//		return fmt.Errorf("boundary generated but not reset")
//	}
//	// the function may be configurable
//	hash := xxh3.Hash(append(key, val...))
//
//	itemSize := len(key) + len(val)
//	ws.totalItemsSize += itemSize
//
//	// can not split
//	if ws.totalItemsSize < ws.config.MinNodeSize {
//		return nil
//	}
//	// must split
//	if ws.totalItemsSize > ws.config.MaxNodeSize {
//		ws.isBoundary = true
//		return nil
//	}
//
//	start := weibullCDF(ws.totalItemsSize-itemSize, ws.config.Strategy.Weilbull.K, ws.config.Strategy.Weilbull.L)
//	end := weibullCDF(ws.totalItemsSize, ws.config.Strategy.Weilbull.K, ws.config.Strategy.Weilbull.L)
//	p := float64(hash) / math.MaxUint32
//	d := 1 - start
//	target := (end - start) / d
//	if p < target {
//		ws.isBoundary = true
//	}
//
//	return nil
//}
//
//func (ws *WeibullSplitter) Reset() {
//	ws.totalItemsSize = 0
//	ws.isBoundary = false
//}
//
//// -exp(-pow(x/L),K)
//func weibullCDF(x int, K, L float64) float64 {
//	return -math.Exp(-math.Pow(float64(x)/L, K))
//}

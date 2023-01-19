package tree

import (
	"encoding/binary"
	"fmt"
	"github.com/multiformats/go-multihash"
	"hash"
)

type Splitter interface {
	// IsBoundary returns whether boundary generated at current location
	IsBoundary() bool
	Append(key, val []byte) error
	Reset()
}

var _ Splitter = &SuffixSplitter{}

type SuffixSplitter struct {
	isBoundary       bool
	totalBytesSize   int
	totalPairsNumber int
	pattern          uint32
	hashFunction     hash.Hash
	config           *TreeConfig
}

func NewSplitterFromConfig(config *TreeConfig) Splitter {
	var splitter Splitter
	switch config.StrategyType {
	case SuffixThreshold:
		hashFunction, err := multihash.GetHasher(config.Strategy.Suffix.HashFunction)
		if err != nil {
			panic(err)
		}
		splitter = &SuffixSplitter{
			config:       config,
			hashFunction: hashFunction,
			pattern:      uint32(1<<config.Strategy.Suffix.ChunkingFactor - 1),
		}
	default:
		panic(fmt.Errorf("unsupported chunk strategy: %v", config.StrategyType))
	}

	return splitter
}

func (p *SuffixSplitter) IsBoundary() bool {
	return p.isBoundary
}

func (p *SuffixSplitter) Append(key, val []byte) error {
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

	// weak check for minNodeSize(in fact the serialized node is a little bigger than the node)
	if p.totalBytesSize < p.config.MinNodeSize {
		return nil
	}

	// the maxNodeSize check is out of splitter and in append function

	p.hashFunction.Write(input)
	h := p.hashFunction.Sum(nil)

	res := binary.BigEndian.Uint32(h)

	if res&p.pattern == 0 {
		p.isBoundary = true
	}
	return nil
}

func (p *SuffixSplitter) Reset() {
	p.totalBytesSize = 0
	p.totalPairsNumber = 0
	p.isBoundary = false
}

//type WeibullSplitter struct {
//	isBoundary     bool
//	totalItemsSize int
//	config         *TreeConfig
//}
//
//func (ws *WeibullSplitter) IsBoundary() bool {
//	return ws.isBoundary
//}
//
//func (ws *WeibullSplitter) Append(Key, Val []byte) error {
//	// can't append until reset splitter after boundary generated
//	if ws.isBoundary {
//		return fmt.Errorf("boundary generated but not reset")
//	}
//	// the function may be configurable
//	hash := xxh3.Hash(append(Key, Val...))
//
//	itemSize := len(Key) + len(Val)
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

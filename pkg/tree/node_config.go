package tree

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 12
)

type ChunkStrategy string

const (
	SuffixThreshold  = 0
	WeibullThreshold = 1
	RollingHash      = 2
)

// TreeConfig includes config for prolly tree, it includes some global setting, the splitter method you choose and specific configs about
// the splitter
type TreeConfig struct {
	MinNodeSize    int
	MaxNodeSize    int
	MaxPairsInNode int
	NodeCodec      NodeCodec
	StrategyType   byte
	Strategy       strategy
}

type NodeCodec struct {
	CidVersion   uint64
	Codec        uint64
	HashFunction uint64
	HashLength   *int
}

func (nc *NodeCodec) ToCidPrefix() *cid.Prefix {
	prefix := &cid.Prefix{
		Version: nc.CidVersion,
		Codec:   nc.Codec,
		MhType:  nc.HashFunction,
	}

	if nc.HashLength != nil {
		prefix.MhLength = *nc.HashLength
	}
	return prefix
}

func (nc *NodeCodec) Equal(another *NodeCodec) bool {
	if nc.HashLength != nil && another.HashLength != nil {
		if *nc.HashLength != *another.HashLength {
			return false
		}
	} else if nc.HashLength == nil && another.HashLength == nil {
	} else {
		return false
	}
	return nc.Codec == another.Codec &&
		nc.HashFunction == another.HashFunction &&
		nc.CidVersion == another.CidVersion
}

func CodecFromCidPrefix(prefix cid.Prefix) NodeCodec {
	var hl *int
	if prefix.MhLength != -1 {
		hl = &prefix.MhLength
	}
	return NodeCodec{
		CidVersion:   prefix.Version,
		Codec:        prefix.Codec,
		HashFunction: prefix.MhType,
		HashLength:   hl,
	}
}

func (cfg *TreeConfig) Equal(another *TreeConfig) bool {
	if cfg.StrategyType != another.StrategyType ||
		cfg.MinNodeSize != another.MinNodeSize ||
		cfg.MaxNodeSize != another.MaxNodeSize {
		return false
	}
	return cfg.NodeCodec.Equal(&another.NodeCodec) &&
		cfg.Strategy.Equal(&another.Strategy, cfg.StrategyType)
}

func DefaultChunkConfig() *TreeConfig {
	return &TreeConfig{
		MinNodeSize:    DefaultMinChunkSize,
		MaxNodeSize:    DefaultMaxChunkSize,
		MaxPairsInNode: 1000,
		StrategyType:   SuffixThreshold,
		NodeCodec:      CodecFromCidPrefix(DefaultLinkProto.Prefix),
		Strategy: strategy{Suffix: &HashThresholdConfig{
			ChunkingFactor: 10,
			HashFunction:   uint64(multicodec.Sha2_256),
		}},
	}
}

type strategy struct {
	//Weilbull    *WeibullThresholdConfig
	//RollingHash *RollingHashConfig
	Suffix *HashThresholdConfig
}

func (sg *strategy) Equal(another *strategy, strategyType byte) bool {
	var strCfg strategyConfig
	var _strCfg strategyConfig
	switch strategyType {
	//case WeibullThreshold:
	//	strCfg = sg.Weilbull
	//	_strCfg = another.Weilbull
	case SuffixThreshold:
		strCfg = sg.Suffix
		_strCfg = another.Suffix
	//case RollingHash:
	//	strCfg = sg.RollingHash
	//	_strCfg = another.RollingHash
	default:
		panic(fmt.Errorf("invalid strategy: %v", strategyType))
	}
	return strCfg.Equal(_strCfg)
}

type strategyConfig interface {
	Equal(sc strategyConfig) bool
}

var _ strategyConfig = &WeibullThresholdConfig{}

type WeibullThresholdConfig struct {
	K float64
	L float64
}

func (wtc *WeibullThresholdConfig) Equal(sc strategyConfig) bool {
	_wtc, ok := sc.(*WeibullThresholdConfig)
	if !ok {
		return false
	}
	if wtc.K == _wtc.K && wtc.L == _wtc.L {
		return true
	}
	return false
}

type HashThresholdConfig struct {
	ChunkingFactor int
	HashFunction   uint64
}

func (ptc *HashThresholdConfig) Equal(sc strategyConfig) bool {
	another, ok := sc.(*HashThresholdConfig)
	if !ok {
		return false
	}
	if ptc.ChunkingFactor == another.ChunkingFactor {
		return true
	}
	return false
}

type RollingHashConfig struct {
	RollingHashWindow uint32
}

func (rhc *RollingHashConfig) Equal(sc strategyConfig) bool {
	another, ok := sc.(*RollingHashConfig)
	if !ok {
		return false
	}
	if rhc.RollingHashWindow == another.RollingHashWindow {
		return true
	}
	return false
}

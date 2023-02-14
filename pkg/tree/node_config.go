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
	CidVersion     uint64
	Codec          uint64
	HashFunction   uint64
	HashLength     *int
	//NodeCodec      NodeCodec
	StrategyType byte
	Strategy     strategy
}

func (cfg *TreeConfig) CidPrefix() *cid.Prefix {
	prefix := &cid.Prefix{
		Version: cfg.CidVersion,
		Codec:   cfg.Codec,
		MhType:  cfg.HashFunction,
	}

	if cfg.HashLength != nil {
		prefix.MhLength = *cfg.HashLength
	}
	return prefix
}

func (cfg *TreeConfig) Equal(another *TreeConfig) bool {
	if cfg.StrategyType != another.StrategyType ||
		cfg.MinNodeSize != another.MinNodeSize ||
		cfg.MaxNodeSize != another.MaxNodeSize ||
		cfg.CidVersion != another.CidVersion ||
		cfg.Codec != another.Codec ||
		cfg.HashFunction != another.HashFunction ||
		cfg.HashLength != another.HashLength {
		return false
	}
	return cfg.Strategy.Equal(&another.Strategy, cfg.StrategyType)
}

func DefaultChunkConfig() *TreeConfig {
	return &TreeConfig{
		MinNodeSize:    DefaultMinChunkSize,
		MaxNodeSize:    DefaultMaxChunkSize,
		MaxPairsInNode: 1000,
		StrategyType:   SuffixThreshold,
		CidVersion:     DefaultLinkProto.Version,
		Codec:          DefaultLinkProto.Codec,
		HashFunction:   DefaultLinkProto.MhType,
		HashLength:     &DefaultLinkProto.MhLength,
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

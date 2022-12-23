package schema

import (
	"fmt"
)

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 12
)

type ChunkStrategy string

const (
	SuffixThreshold  byte = byte(0)
	WeibullThreshold byte = byte(1)
	RollingHash      byte = byte(2)
)

// TreeConfig includes config for prolly tree, it includes some global setting, the splitter method you choose and specific configs about
// the splitter
type TreeConfig struct {
	StrategyType   byte
	MinNodeSize    int
	MaxNodeSize    int
	MaxPairsInNode int
	NodeCodec      []byte
	Strategy       strategy
}

func (cfg *TreeConfig) Equal(_cfg *TreeConfig) bool {
	if cfg.StrategyType != _cfg.StrategyType ||
		cfg.MinNodeSize != _cfg.MinNodeSize ||
		cfg.MaxNodeSize != _cfg.MaxNodeSize {
		return false
	}
	return cfg.Strategy.Equal(&_cfg.Strategy, cfg.StrategyType)
}

func DefaultChunkConfig() *TreeConfig {
	return &TreeConfig{
		MinNodeSize:    DefaultMinChunkSize,
		MaxNodeSize:    DefaultMaxChunkSize,
		MaxPairsInNode: 1000,
		StrategyType:   SuffixThreshold,
		NodeCodec:      DefaultLinkProto.Prefix.Bytes(),
		Strategy: strategy{Suffix: &PrefixThresholdConfig{
			ChunkingFactor: 10,
		}},
	}
}

type strategy struct {
	//Weilbull    *WeibullThresholdConfig
	//RollingHash *RollingHashConfig
	Suffix *PrefixThresholdConfig
}

func (sg *strategy) Equal(_sg *strategy, strategyType byte) bool {
	var strCfg strategyConfig
	var _strCfg strategyConfig
	switch strategyType {
	//case WeibullThreshold:
	//	strCfg = sg.Weilbull
	//	_strCfg = _sg.Weilbull
	case SuffixThreshold:
		strCfg = sg.Suffix
		_strCfg = _sg.Suffix
	//case RollingHash:
	//	strCfg = sg.RollingHash
	//	_strCfg = _sg.RollingHash
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

type PrefixThresholdConfig struct {
	ChunkingFactor int
}

func (ptc *PrefixThresholdConfig) Equal(sc strategyConfig) bool {
	_ptc, ok := sc.(*PrefixThresholdConfig)
	if !ok {
		return false
	}
	if ptc.ChunkingFactor == _ptc.ChunkingFactor {
		return true
	}
	return false
}

type RollingHashConfig struct {
	RollingHashWindow uint32
}

func (rhc *RollingHashConfig) Equal(sc strategyConfig) bool {
	_rhc, ok := sc.(*RollingHashConfig)
	if !ok {
		return false
	}
	if rhc.RollingHashWindow == _rhc.RollingHashWindow {
		return true
	}
	return false
}

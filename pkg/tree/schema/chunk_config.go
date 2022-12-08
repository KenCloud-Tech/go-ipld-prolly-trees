package schema

import "fmt"

const (
	DefaultMinChunkSize = 1 << 9
	DefaultMaxChunkSize = 1 << 14
)

type ChunkStrategy string

const (
	WeibullThreshold ChunkStrategy = "WeibullThreshold"
	RollingHash      ChunkStrategy = "RollingHash"
	PrefixThreshold  ChunkStrategy = "PrefixThreshold"
)

// Chunk Config for prolly tree, it includes some global setting, the splitter method you choose and specific configs about
// the splitter
type ChunkConfig struct {
	ChunkStrategy ChunkStrategy
	MinNodeSize   int
	MaxNodeSize   int
	Strategy      strategy
}

func (cfg *ChunkConfig) Equal(_cfg *ChunkConfig) bool {
	if cfg.ChunkStrategy != _cfg.ChunkStrategy ||
		cfg.MinNodeSize != _cfg.MinNodeSize ||
		cfg.MaxNodeSize != _cfg.MaxNodeSize {
		return false
	}
	return cfg.Strategy.Equal(&_cfg.Strategy, cfg.ChunkStrategy)
}

func DefaultChunkConfig() *ChunkConfig {
	return &ChunkConfig{
		MinNodeSize:   DefaultMinChunkSize,
		MaxNodeSize:   DefaultMaxChunkSize,
		ChunkStrategy: WeibullThreshold,
		Strategy: strategy{Weilbull: &WeibullThresholdConfig{
			4, 4096,
		}},
	}
}

type strategy struct {
	Weilbull    *WeibullThresholdConfig
	RollingHash *RollingHashConfig
	Prefix      *PrefixThresholdConfig
}

func (sg *strategy) Equal(_sg *strategy, name ChunkStrategy) bool {
	var strCfg strategyConfig
	var _strCfg strategyConfig
	switch name {
	case WeibullThreshold:
		strCfg = sg.Weilbull
		_strCfg = _sg.Weilbull
	case PrefixThreshold:
		strCfg = sg.Prefix
		_strCfg = _sg.Prefix
	case RollingHash:
		strCfg = sg.RollingHash
		_strCfg = _sg.RollingHash
	default:
		panic(fmt.Errorf("invalid strategy: %s", name))
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
	chunkingFactor int
}

func (ptc *PrefixThresholdConfig) Equal(sc strategyConfig) bool {
	_ptc, ok := sc.(*PrefixThresholdConfig)
	if !ok {
		return false
	}
	if ptc.chunkingFactor == _ptc.chunkingFactor {
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

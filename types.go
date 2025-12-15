package zrank

import (
	"time"
)

const localCacheKeyPrefix = "rank:local:%s:%s"

type IRanker interface {
	// Push 写入排行榜数据
	Push(memberID, val, usedTs int64) error
	// FetchRankList 返回排行榜
	FetchRankList(start, stop int64) ([]RankMember, error)
	FetchRankTopList() ([]RankMember, error)
	// FetchRankByMember 返回用户排名
	FetchRankByMember(memberID int64) (int64, error)
	// FetchScoreByMember 返回用户排行值
	FetchScoreByMember(memberID int64) (int64, error)
	// RankCount 返回排行榜用户数量
	RankCount() int64
	// Calibration 校准排行榜
	Calibration() error
	// Stop 停止排行榜
	Stop()
}

type WALConfig struct {
	Dir             string
	SnapshotTick    time.Duration
	FlushTick       time.Duration
	MaxApplyBatch   int
	CompactInterval time.Duration
}

type RankOptions struct {
	CalibrationConfig *CalibrationConfig
	LocalCacheExpire  time.Duration
	WALConfig         *WALConfig
	TopListSize       int64 // 缓存前N名列表大小
}

type RankOption func(o *RankOptions)

func WithCalibration(calibration *CalibrationConfig) RankOption {
	return func(o *RankOptions) {
		o.CalibrationConfig = calibration
	}
}

func WithLocalCacheExpire(expire time.Duration) RankOption {
	return func(o *RankOptions) {
		o.LocalCacheExpire = expire
	}
}

func WithWAL(config *WALConfig) RankOption {
	return func(o *RankOptions) {
		o.WALConfig = config
	}
}

func WithTopListCache(size int64) RankOption {
	return func(o *RankOptions) {
		o.TopListSize = size
	}
}

// PeriodInfo 榜单周期信息
type PeriodInfo struct {
	Key      string // 榜单Redis Key
	StartTs  int64  // 周期开始时间戳
	EndTs    int64  // 周期结束时间戳
	SettleTs int64  // 结算时间戳
}

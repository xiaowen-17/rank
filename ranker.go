package zrank

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/spf13/cast"
	"github.com/xiaowen-17/go-redisx"
	"github.com/xiaowen-17/log"
	"github.com/zeromicro/go-zero/core/collection"
)

const luaScript = `
		-- 排行榜key
		local key = KEYS[1]
		-- 要更新的成员id
		local uid = ARGV[1]
		-- 成员本次新增的val（小数位为时间差标识）
		local valScore = ARGV[2]

		-- 获取成员之前的score
		local score = redis.call("ZSCORE", key, uid)
		if score == false then
			score = 0
		end
		-- 从score中抹除用于时间差标识的小数部分，获取整数的排序val
		local val = math.floor(score)

		-- 更新成员最新的score信息（累计val.最新时间差）
		local newScore = valScore+val
		redis.call("ZADD", key, newScore, uid)

		-- 更新成功返回newScore（使用tostring返回小数）
		return tostring(newScore)
	`

/*
利用 double 的浮点计算
使用redis zset，得分相同时，按时间先后进行排序，用户排行名次Rank从1开始；
将zset score按十进制数拆分，score十进制数字总共固定为16位（超过16位的数会有浮点数精度导致进位的问题），
整数部分用于表示用户排序值val，小数部分表示活动总时长的时间戳（秒）与用户参与活动用时时间戳（秒）的差值deltaTs，
小数部分的数字长度由deltaTs的数字长度确定，整数部分最大支持长度则为：16-len(deltaTs)。
比如活动时长为10天，总时间差为864000，长度为6，则deltaTs宽度为6，不够则在前面补0

分数的设计：
整数 = 得分
小数 = 活动总时长的时间戳-用户参与活动用时时间戳
*/

// RankRouter 排行榜路由
type RankRouter struct {
	RdsCli            *redisx.RedisManager
	Name              string
	Key               string
	StartTs           int64
	EndTs             int64
	TimePadWidth      int // 活动结束时间与用户排序值更新时间的差值补0宽度
	CalibrationConfig *CalibrationConfig
	cache             *collection.Cache
	writer            *RankWriter    // 内部聚合写入器
	topListSize       int64          // 缓存前N名列表大小
	PeriodManager     *PeriodManager // 周期管理器(总榜为nil)
}

// RankMember 排行榜成员
type RankMember struct {
	MemberID int64
	Score    int64
	Rank     int64
}

// CalibrationConfig 校准配置
type CalibrationConfig struct {
	CronExpr      string
	Size          int64 // 校准数量（前N名）
	ScoreProvider func(ctx context.Context, memberKey string) (int64, time.Time, error)
}

// LocalCacheConfig 本地缓存配置
type LocalCacheConfig struct {
	Expire      time.Duration
	TopListSize int64 // 缓存前N名列表
	TopListKey  string
}

const (
	TopListKey = "toplist"
	ScoreKey   = "score:%d"
	RankKey    = "rank:%d"
)

// NewRankRouter 创建排行榜路由
func NewRankRouter(rds *redisx.RedisManager, name string, periodInfo PeriodInfo, opts ...RankOption) *RankRouter {
	deltaTs := periodInfo.EndTs - periodInfo.StartTs
	timePadWidth := 0

	// 计算时间戳宽度，并检查精度
	if deltaTs > 0 {
		timePadWidth = len(fmt.Sprint(deltaTs))

		// float64 有效数字约16位，需要预留给整数部分
		// 如果时间戳宽度过大，会导致精度问题
		if timePadWidth > 8 {
			log.Warnf("RankRouter %s: deltaTs=%d 宽度=%d 过大，可能导致精度问题！建议使用较短的时间范围或不使用时间权重",
				name, deltaTs, timePadWidth)
		}
	}

	o := newOptions(opts...)

	router := &RankRouter{
		RdsCli:            rds,
		Name:              name,
		Key:               periodInfo.Key,
		StartTs:           periodInfo.StartTs,
		EndTs:             periodInfo.EndTs,
		TimePadWidth:      timePadWidth,
		CalibrationConfig: o.CalibrationConfig,
		topListSize:       o.TopListSize,
	}

	router.PeriodManager = NewPeriodManager(periodInfo)

	if o.LocalCacheExpire > 0 {
		cache, err := collection.NewCache(o.LocalCacheExpire)
		if err != nil {
			log.Errorf("RankRouter开启本地缓存失败 err: %v", err)
			return router
		}
		router.cache = cache
	}

	if o.CalibrationConfig != nil {
		router.startCalibration()
	}

	// 如果配置了WAL，启动RankWriter
	if o.WALConfig != nil {
		writer, err := newRankWriter(router, o.WALConfig.Dir, o.WALConfig.SnapshotTick, o.WALConfig.FlushTick, o.WALConfig.MaxApplyBatch, o.WALConfig.CompactInterval)
		if err != nil {
			log.Errorf("RankRouter启动WAL失败 err: %v", err)
			return router
		}
		router.writer = writer
	}

	return router
}

func newOptions(opts ...RankOption) RankOptions {
	var o RankOptions
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

// Push 业务入口：写入排行榜数据
func (r *RankRouter) Push(memberID, val, usedTs int64) error {
	// 如果启用了WAL，通过writer聚合写入
	if r.writer != nil {
		return r.writer.Push(memberID, val, usedTs)
	}
	// 否则直接写入Redis
	_, err := r.flush(memberID, val, usedTs)
	return err
}

// flush 内部方法：直接刷新到Redis
func (r *RankRouter) flush(memberID, val int64, usedTs int64) (float64, error) {
	keys := []string{r.Key}
	args := []interface{}{memberID, r.valToScore(val, usedTs)}
	result := r.RdsCli.Eval(luaScript, keys, args)
	if !result.IsOK() {
		return 0, fmt.Errorf("rank %s key: %s flush failed:  %w", r.Name, r.Key, result.Err)
	}

	score := cast.ToFloat64(result.Val)

	return score, nil
}

// Stop 停止排行榜（关闭WAL）
func (r *RankRouter) Stop() {
	if r.writer != nil {
		r.writer.Stop()
	}
}

// SetRedisExpire 为Redis ZSET设置过期时间(用于归档榜单)
func (r *RankRouter) SetRedisExpire(ttl time.Duration) error {
	result := r.RdsCli.Expire(r.Key, ttl)
	if !result.IsOK() {
		return fmt.Errorf("set expire for key %s failed: %w", r.Key, result.Err)
	}
	log.Infof("RankRouter %s: Redis key %s expires in %v", r.Name, r.Key, ttl)
	return nil
}

// GetWriter 获取内部writer(用于调度器访问)
func (r *RankRouter) GetWriter() *RankWriter {
	return r.writer
}

// val 转为 score:
// score = float64(val.deltaTs)
// 整数部分固定最大10位（9999999999），小数部分根据时间范围动态调整
func (r *RankRouter) valToScore(val int64, usedTs int64) float64 {
	// 限制整数部分最大值，防止溢出
	const maxVal = 9999999999 // 10位整数
	if val > maxVal {
		val = maxVal
	}

	// 如果没有时间权重（timePadWidth=0），直接返回分数
	if r.TimePadWidth == 0 {
		return float64(val)
	}

	// 计算时间差（越早参与，deltaTs 越大，排名越靠前）
	deltaTs := r.EndTs - r.StartTs - usedTs
	if deltaTs < 0 {
		deltaTs = 0
	}

	// 构造 score = val.deltaTs
	scoreFormat := fmt.Sprintf("%%v.%%0%dd", r.TimePadWidth)
	scoreStr := fmt.Sprintf(scoreFormat, val, deltaTs)
	score := cast.ToFloat64(scoreStr)
	return score
}

// scoreToVal 从编码的 score 中提取整数部分（实际分数）
func (r *RankRouter) scoreToVal(score float64) int64 {
	return int64(math.Floor(score))
}

func (r *RankRouter) getLocalCacheKey(key string) string {
	return fmt.Sprintf(localCacheKeyPrefix, r.Key, key)
}

// FetchRankList 获取排行榜列表
func (r *RankRouter) FetchRankList(start, stop int64) ([]RankMember, error) {
	result := r.RdsCli.ZRevRangeWithScores(r.Key, start, stop)
	if !result.IsOK() {
		return nil, fmt.Errorf("rank %s key: %s fetch rank list failed: %w", r.Name, r.Key, result.Err)
	}

	members := make([]RankMember, 0, len(result.Val))
	for i, z := range result.Val {
		members = append(members, RankMember{
			MemberID: cast.ToInt64(z.Member),
			Score:    r.scoreToVal(z.Score),
			Rank:     start + int64(i) + 1,
		})
	}

	return members, nil
}

// FetchRankTopList 获取排行榜前N名
func (r *RankRouter) FetchRankTopList() ([]RankMember, error) {
	if r.cache != nil {
		cacheKey := r.getLocalCacheKey(TopListKey)
		members, err := r.cache.Take(cacheKey, func() (any, error) {
			return r.FetchRankList(0, r.topListSize-1)
		})
		if err != nil {
			return nil, err
		}

		list := members.([]RankMember)
		return list, nil
	}

	members, err := r.FetchRankList(0, r.topListSize-1)
	if err != nil {
		return nil, err
	}

	return members, nil
}

func (r *RankRouter) fetchRankByMember(memberID int64) (int64, error) {
	result := r.RdsCli.ZRevRank(r.Key, cast.ToString(memberID))
	if !result.IsOK() {
		return 0, fmt.Errorf("rank %s key: %s fetch rank by member failed: %w", r.Name, r.Key, result.Err)
	}

	rank := result.Val + 1

	return rank, nil
}

// FetchRankByMember 获取成员排名
func (r *RankRouter) FetchRankByMember(memberID int64) (int64, error) {
	// 先从缓存中获取
	if r.cache != nil {
		cacheKey := r.getLocalCacheKey(fmt.Sprintf(RankKey, memberID))
		rank, err := r.cache.Take(cacheKey, func() (any, error) {
			return r.fetchRankByMember(memberID)
		})

		if err != nil {
			return 0, err
		}

		rankInt := rank.(int64)
		return rankInt, nil
	}
	rank, err := r.fetchRankByMember(memberID)
	if err != nil {
		return 0, err
	}

	return rank, nil
}

func (r *RankRouter) fetchScoreByMember(memberID int64) (int64, error) {
	result := r.RdsCli.ZScore(r.Key, cast.ToString(memberID))
	if !result.IsOK() {
		return 0, fmt.Errorf("rank %s key: %s fetch score by member failed: %w", r.Name, r.Key, result.Err)
	}

	score := r.scoreToVal(result.Val)

	return score, nil
}

// FetchScoreByMember 获取成员分数
func (r *RankRouter) FetchScoreByMember(memberID int64) (int64, error) {
	// 先从缓存中获取
	if r.cache != nil {
		cacheKey := r.getLocalCacheKey(fmt.Sprintf(ScoreKey, memberID))
		score, err := r.cache.Take(cacheKey, func() (any, error) {
			return r.fetchScoreByMember(memberID)
		})

		if err != nil {
			return 0, err
		}

		scoreInt := score.(int64)
		return scoreInt, nil
	}
	score, err := r.fetchScoreByMember(memberID)
	if err != nil {
		return 0, err
	}

	return score, nil
}

// RankCount 获取排行榜总人数
func (r *RankRouter) RankCount() int64 {
	result := r.RdsCli.ZCard(r.Key)
	if !result.IsOK() {
		log.Errorf("rank %s key: %s fetch rank count failed: %v", r.Name, r.Key, result.Err)
		return 0
	}
	return result.Val
}

// startCalibration 启动定时校准
func (r *RankRouter) startCalibration() {
	// if r.CalibrationConfig == nil || r.CalibrationConfig.CronExpr == "" {
	// 	return
	// }

	// // TODO: 使用 cron + 分布式锁 实现定时任务
	// // 这里先用简单的 goroutine + ticker 实现
	// go func() {
	// 	// 立即执行一次
	// 	if err := r.calibration(); err != nil {
	// 		log.Errorf("calibration failed: %v", err)
	// 	}

	// 	// TODO: 解析 CronExpr 并按 cron 表达式执行
	// 	// 暂时使用固定间隔（需要引入 cron 库）
	// 	ticker := time.NewTicker(5 * time.Minute)
	// 	defer ticker.Stop()

	// 	for range ticker.C {
	// 		if err := r.calibration(); err != nil {
	// 			log.Errorf("calibration failed: %v", err)
	// 		}
	// 	}
	// }()
}

// calibration 校准排行榜（重建前N名）
func (r *RankRouter) calibration() error {
	if r.CalibrationConfig == nil || r.CalibrationConfig.ScoreProvider == nil {
		return fmt.Errorf("calibration config or score provider is nil")
	}

	size := r.CalibrationConfig.Size
	if size <= 0 {
		size = 100
	}

	ctx := context.Background()
	topMembers, err := r.FetchRankList(0, size-1)
	if err != nil {
		return fmt.Errorf("fetch top %d members failed: %w", size, err)
	}

	for _, member := range topMembers {
		memberKey := cast.ToString(member.MemberID)
		score, ts, err := r.CalibrationConfig.ScoreProvider(ctx, memberKey)
		if err != nil {
			log.Errorf("calibration get score failed for member %s: %v", memberKey, err)
			continue
		}

		usedTs := ts.Unix() - r.StartTs
		_, err = r.flush(member.MemberID, score, usedTs)
		if err != nil {
			log.Errorf("calibration flush failed for member %s: %v", memberKey, err)
		}
	}

	return nil
}

// Calibration 手动触发校准（实现 IRanker 接口）
func (r *RankRouter) Calibration() error {
	return r.calibration()
}

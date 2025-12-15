package zrank

import (
	"sync"
)

type scoreAgg struct {
	Val     int64
	UsedTs  int64
	Updates int64
}

type ScoreAggregator struct {
	data map[string]*scoreAgg
	mu   sync.RWMutex
}

func newScoreAggregator() *ScoreAggregator {
	return &ScoreAggregator{
		data: make(map[string]*scoreAgg),
	}
}

// Add 将一个更新添加到聚合器中
func (a *ScoreAggregator) Add(memberKey string, val, usedTs int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	agg, ok := a.data[memberKey]
	if !ok {
		agg = &scoreAgg{UsedTs: usedTs}
		a.data[memberKey] = agg
	}
	agg.Val += val
	if usedTs < agg.UsedTs {
		agg.UsedTs = usedTs
	}
	agg.Updates++
}

// RestoreBatch 批量恢复数据（避免频繁加锁）
func (a *ScoreAggregator) RestoreBatch(data map[string]*scoreAgg) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range data {
		if existing, ok := a.data[k]; ok {
			existing.Val += v.Val
			if v.UsedTs < existing.UsedTs {
				existing.UsedTs = v.UsedTs
			}
			existing.Updates += v.Updates
		} else {
			a.data[k] = v
		}
	}
}

// Drain 将当前聚合器中的所有更新提取出来，清空聚合器
func (a *ScoreAggregator) Drain() map[string]*scoreAgg {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := a.data
	a.data = make(map[string]*scoreAgg)
	return out
}

func (a *ScoreAggregator) GetAll() map[string]*scoreAgg {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.data
}

// Size 返回当前聚合条目数量
func (a *ScoreAggregator) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.data)
}

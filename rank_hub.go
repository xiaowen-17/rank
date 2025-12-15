package zrank

import "sync"

type RankHub struct {
	ranks map[string]IRanker
	mu    sync.RWMutex
}

func NewRankHub() *RankHub {
	return &RankHub{
		ranks: make(map[string]IRanker),
	}
}

func (h *RankHub) Register(name string, r IRanker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ranks[name] = r
}

func (h *RankHub) Get(name string) (IRanker, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.ranks[name]
	return r, ok
}

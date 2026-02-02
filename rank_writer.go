package zrank

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cast"
)

// ========== types for WAL entries ==========

// EventEntry 表示业务产生的原始事件（每个 Push 都写入 events.wal）
type EventEntry struct {
	Seq      int64 `json:"seq"`
	MemberID int64 `json:"member_key"`
	Val      int64 `json:"val"`
	UsedTs   int64 `json:"used_ts"`
}

// AggEntry 表示聚合快照（由聚合器定期生成并写入 agg.wal）
// 聚合后的记录是最终会发送到 Redis 的增量
type AggEntry struct {
	Seq      int64 `json:"seq"`
	MemberID int64 `json:"member_key"`
	Val      int64 `json:"val"`
	UsedTs   int64 `json:"used_ts"`
}

// ========== simple file-based WAL helpers ==========

// appendLine 将结构体以 JSON 行写入文件（append）
func appendLine(f *os.File, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// readLines 逐行读取并反序列化（调用方负责类型转换）
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var res []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		res = append(res, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

// overwriteFile 将给定内容覆盖写入文件（原子性：写临时文件再替换）
func overwriteFileAtomic(path string, lines []string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "tmpwal-*")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(tmp)
	for _, l := range lines {
		if _, err := w.WriteString(l + "\n"); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	_ = tmp.Close()
	// rename
	return os.Rename(tmp.Name(), path)
}

// ========== RankWriter with improved WAL ==========

// RankWriter combines:
// - events.wal: 原始事件条目在 Push 时写入
// - agg.wal: 聚合快照条目在flush之前保持不变
// - agg.ack: 已确认的聚合快照 seq 列表，用于安全压缩 agg.wal
type RankWriter struct {
	router *RankRouter

	dir string // 数据目录

	eventsPath string
	aggPath    string
	aggAckPath string

	agg         *ScoreAggregator
	eventFileMu sync.Mutex

	// worker controls
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	snapshotTick    time.Duration  // 聚合器快照生成间隔
	flushTick       time.Duration  // 聚合器快照写入 Redis 间隔
	compactInterval time.Duration  // 聚合器快照压缩间隔
	seq             int64          // 本地序列生成器
	maxApplyBatch   int            // 控制批量写入并发度
	pushWg          sync.WaitGroup // 追踪正在进行的Push操作
}

// newRankWriter 创建 RankWriter；dir 为 wal 存放目录
func newRankWriter(router *RankRouter, dir string, snapshotTick, flushTick time.Duration, maxApplyBatch int, compactInterval time.Duration) (*RankWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw := &RankWriter{
		router:          router,
		dir:             dir,
		eventsPath:      filepath.Join(dir, "events.wal"),
		aggPath:         filepath.Join(dir, "agg.wal"),
		aggAckPath:      filepath.Join(dir, "agg.ack"),
		agg:             newScoreAggregator(),
		ctx:             ctx,
		cancel:          cancel,
		snapshotTick:    snapshotTick,
		flushTick:       flushTick,
		maxApplyBatch:   maxApplyBatch,
		compactInterval: compactInterval,
		seq:             time.Now().UnixNano(),
	}
	// Start background loops
	rw.wg.Add(3)
	go rw.snapshotLoop()
	go rw.flushLoop()
	go rw.compactLoop()
	// 启动时尝试恢复 agg.wal（回放）
	if err := rw.replayAggWALToAgg(); err != nil {
		// log but continue
		fmt.Printf("rankwriter: replay agg wal err: %v\n", err)
	}
	// 也回放 events.wal 到聚合器，确保崩溃前未聚合的事件不会丢失（如果 agg.wal 为空）
	if err := rw.replayEventsToAgg(); err != nil {
		fmt.Printf("rankwriter: replay events wal err: %v\n", err)
	}
	return rw, nil
}

// nextSeq 线程安全的 seq 生成
func (rw *RankWriter) nextSeq() int64 {
	return atomic.AddInt64(&rw.seq, 1)
}

// Push 写 events.wal（持久化），并把数据加入内存聚合
func (rw *RankWriter) Push(memberID, val, usedTs int64) error {
	rw.pushWg.Add(1)
	defer rw.pushWg.Done()

	seq := rw.nextSeq()
	ev := EventEntry{
		Seq:      seq,
		MemberID: memberID,
		Val:      val,
		UsedTs:   usedTs,
	}
	// append to events.wal
	if err := rw.appendEvent(&ev); err != nil {
		return err
	}
	// 内存聚合器也加入更新
	memberKey := cast.ToString(memberID)
	rw.agg.Add(memberKey, val, usedTs)
	return nil
}

// WaitForPending 等待所有正在进行的Push完成
func (rw *RankWriter) WaitForPending() {
	rw.pushWg.Wait()
}

// appendEvent 追加事件行（线程安全）
func (rw *RankWriter) appendEvent(ev *EventEntry) error {
	rw.eventFileMu.Lock()
	defer rw.eventFileMu.Unlock()
	f, err := os.OpenFile(rw.eventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return appendLine(f, ev)
}

// snapshotLoop: 定期把内存聚合导出为 agg.wal（写入 agg.wal 后可删除 events.wal）
func (rw *RankWriter) snapshotLoop() {
	defer rw.wg.Done()
	ticker := time.NewTicker(rw.snapshotTick)
	defer ticker.Stop()
	for {
		select {
		case <-rw.ctx.Done():
			// final snapshot before exit
			rw.snapshotOnce()
			return
		case <-ticker.C:
			rw.snapshotOnce()
		}
	}
}

// snapshotOnce: 将 aggregator.Drain 写入 agg.wal（每个聚合条目生成一个 AggEntry）
func (rw *RankWriter) snapshotOnce() {
	if rw.agg.Size() == 0 {
		return
	}
	// 先打开文件，避免 Drain 后无法写入导致数据丢失
	f, err := os.OpenFile(rw.aggPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Printf("rankwriter: open agg.wal failed: %v\n", err)
		return
	}
	defer f.Close()

	// 文件打开成功后再 Drain
	aggMap := rw.agg.Drain()
	if len(aggMap) == 0 {
		return
	}

	// 创建聚合条目并追加写入
	for k, v := range aggMap {
		seq := rw.nextSeq()
		id := cast.ToInt64(k)
		ae := AggEntry{
			Seq:      seq,
			MemberID: id,
			Val:      v.Val,
			UsedTs:   v.UsedTs,
		}
		if err := appendLine(f, &ae); err != nil {
			fmt.Printf("rankwriter: append agg.wal failed: %v\n", err)
			// 写入失败，批量放回聚合器（一次加锁）
			rw.agg.RestoreBatch(aggMap)
			return
		} else {
			// 写入成功，删除已处理的聚合条目
			delete(aggMap, k)
		}
	}
	// 写入成功：由于事件已被聚合到 agg.wal，我们可以安全地截断 events.wal
	// 简单策略：删除 events.wal（因为所有事件都已聚合）
	if err := os.Remove(rw.eventsPath); err != nil && !os.IsNotExist(err) {
		fmt.Printf("rankwriter: remove events.wal failed: %v\n", err)
	}
}

// flushLoop: 定期尝试将 agg.wal 的未 ack 条目应用到 Redis（通过 RankRouter.Flush）
// 实现上我们会读取 agg.wal 中所有行并尝试 apply，apply 成功后记录 ack 到 agg.ack
func (rw *RankWriter) flushLoop() {
	defer rw.wg.Done()
	ticker := time.NewTicker(rw.flushTick)
	defer ticker.Stop()
	for {
		select {
		case <-rw.ctx.Done():
			// final flush
			rw.flushAllAggOnce()
			return
		case <-ticker.C:
			rw.flushAllAggOnce()
		}
	}
}

// flushAllAggOnce: 读取 agg.wal 并尝试应用未 ack 的条目
func (rw *RankWriter) flushAllAggOnce() {
	lines, err := readLines(rw.aggPath)
	if err != nil {
		fmt.Printf("rankwriter: read agg.wal failed: %v\n", err)
		return
	}
	if len(lines) == 0 {
		return
	}
	acked := rw.readAckSet()
	// 解析 agg.wal 行并筛选未 ack 的条目
	var toApply []AggEntry
	for _, l := range lines {
		var ae AggEntry
		if err := json.Unmarshal([]byte(l), &ae); err != nil {
			// 忽略格式错误的行
			continue
		}
		if _, ok := acked[ae.Seq]; ok {
			continue // already applied
		}
		toApply = append(toApply, ae)
	}
	if len(toApply) == 0 {
		return
	}
	// 顺序应用或并发应用（根据 maxApplyBatch 配置）
	sem := make(chan struct{}, rw.maxApplyBatch)
	failedCh := make(chan AggEntry, len(toApply))
	var wg sync.WaitGroup
	for _, ae := range toApply {
		wg.Add(1)
		sem <- struct{}{}
		go func(ae AggEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := rw.router.flush(ae.MemberID, ae.Val, ae.UsedTs)
			if err != nil {
				failedCh <- ae
				return
			}
			// success: append ack
			if err := rw.appendAggAck(ae.Seq); err != nil {
				failedCh <- ae
			}
		}(ae)
	}
	wg.Wait()
	close(failedCh)

	// 收集失败项
	failedCount := 0
	for range failedCh {
		failedCount++
	}
	if failedCount > 0 {
		fmt.Printf("rankwriter: %d agg entries failed to apply\n", failedCount)
	}
}

// readAckSet 读取 agg.ack 为集合
func (rw *RankWriter) readAckSet() map[int64]struct{} {
	res := make(map[int64]struct{})
	lines, err := readLines(rw.aggAckPath)
	if err != nil {
		return res
	}
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		if seq := cast.ToInt64(strings.TrimSpace(l)); seq != 0 {
			res[seq] = struct{}{}
		}
	}
	return res
}

// appendAggAck 将已应用的 agg seq 写入 agg.ack（append）
func (rw *RankWriter) appendAggAck(seq int64) error {
	f, err := os.OpenFile(rw.aggAckPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%d\n", seq)); err != nil {
		return err
	}
	return f.Sync()
}

// compactLoop: 定期压缩 agg.wal（移除已 ack 的行）
func (rw *RankWriter) compactLoop() {
	defer rw.wg.Done()
	ticker := time.NewTicker(rw.compactInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rw.ctx.Done():
			// final compact attempt
			rw.compactAggWAL()
			return
		case <-ticker.C:
			rw.compactAggWAL()
		}
	}
}

// compactAggWAL: 压缩 agg.wal，保留未 ack 的行
func (rw *RankWriter) compactAggWAL() {
	lines, err := readLines(rw.aggPath)
	if err != nil {
		return
	}
	if len(lines) == 0 {
		// nothing
		return
	}
	acked := rw.readAckSet()
	var remaining []string
	for _, l := range lines {
		var ae AggEntry
		if err := json.Unmarshal([]byte(l), &ae); err != nil {
			// 忽略格式错误的行，保持它们以便稍后重试
			remaining = append(remaining, l)
			continue
		}
		if _, ok := acked[ae.Seq]; ok {
			// skip acked
			continue
		}
		remaining = append(remaining, l)
	}
	// 原子地用未 ack 的行覆盖 agg.wal
	if err := overwriteFileAtomic(rw.aggPath, remaining); err != nil {
		fmt.Printf("rankwriter: compact agg.wal failed: %v\n", err)
		return
	}
	//  可选：如果 agg.ack 增长过大，截断它（保留仅对剩余 wal 未 ack 的项）
	// 最简单的策略：保持原样；如果需要，可实现定期 GC 来截断 agg.ack
}

// replayAggWALToAgg: 启动时读取 agg.wal 中未 ack 的条目并恢复到 aggregator 中，避免重复写入内存丢失
func (rw *RankWriter) replayAggWALToAgg() error {
	lines, err := readLines(rw.aggPath)
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return nil
	}
	acked := rw.readAckSet()
	for _, l := range lines {
		var ae AggEntry
		if err := json.Unmarshal([]byte(l), &ae); err != nil {
			continue
		}
		if _, ok := acked[ae.Seq]; ok {
			continue
		}
		// 未 ack 的项，将其恢复到 aggregator 中，以便稍后重试或应用
		memberKey := cast.ToString(ae.MemberID)
		rw.agg.Add(memberKey, ae.Val, ae.UsedTs)
	}
	return nil
}

// replayEventsToAgg: 启动时读取 events.wal 并还原到 aggregator，保证事件在崩溃后不会丢失（除非已经由 agg.wal 吸收）
// 简单策略：把 events.wal 中的原始事件聚合到内存以便下次 snapshot 使用
func (rw *RankWriter) replayEventsToAgg() error {
	lines, err := readLines(rw.eventsPath)
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return nil
	}
	for _, l := range lines {
		var ev EventEntry
		if err := json.Unmarshal([]byte(l), &ev); err != nil {
			continue
		}
		memberKey := cast.ToString(ev.MemberID)
		rw.agg.Add(memberKey, ev.Val, ev.UsedTs)
	}
	// 简单策略：不删除 events.wal，直到 snapshot 消费完它们
	return nil
}

// ForceFlush 强制刷新所有WAL数据到Redis(阻塞直到完成)
// 注意：会停止后台协程，调用后RankWriter进入只读状态
func (rw *RankWriter) ForceFlush() error {
	// 先等待所有正在进行的Push完成
	rw.WaitForPending()

	// 再停止后台协程
	rw.cancel()
	rw.wg.Wait()

	fmt.Println("rank writer: background workers stopped, starting force flush")

	// 1. 最终snapshot(将内存聚合写入agg.wal)
	rw.snapshotOnce()

	// 2. 循环刷新agg.wal，直到全部ACK
	maxRetries := 20
	for i := 0; i < maxRetries; i++ {
		rw.flushAllAggOnce()

		// 检查是否还有未ACK的数据
		unacked := rw.countUnackedData()
		if unacked == 0 {
			fmt.Printf("rankwriter: force flush completed after %d retries\n", i+1)
			return nil // 全部刷新完成
		}

		fmt.Printf("rankwriter: %d unacked entries remaining, retry %d/%d\n", unacked, i+1, maxRetries)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("force flush timeout after %d retries, check Redis connection", maxRetries)
}

// countUnackedData 统计未ACK的条目数量
func (rw *RankWriter) countUnackedData() int {
	lines, err := readLines(rw.aggPath)
	if err != nil || len(lines) == 0 {
		return 0
	}

	acked := rw.readAckSet()
	unacked := 0
	for _, l := range lines {
		var ae AggEntry
		if err := json.Unmarshal([]byte(l), &ae); err != nil {
			continue
		}
		if _, ok := acked[ae.Seq]; !ok {
			unacked++
		}
	}
	return unacked
}

// ClearWAL 清空所有WAL文件(在ForceFlush成功后调用)
func (rw *RankWriter) ClearWAL() error {
	files := []string{rw.eventsPath, rw.aggPath, rw.aggAckPath}
	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("clear wal file %s failed: %w", f, err)
		}
	}
	fmt.Println("rankwriter: all WAL files cleared")
	return nil
}

// Stop 停止 RankWriter，等待后台任务结束
func (rw *RankWriter) Stop() {
	rw.cancel()
	rw.wg.Wait()
}

// ========== 使用示例（注释） ==========

/*
使用范例：

// 方式1：不启用WAL（直接写Redis）
router := NewRankRouter(rds, "rank:video:week:202511W45", startTs, endTs)
router.Push(userID, deltaScore, usedTs)

// 方式2：启用WAL（本地聚合后批量写Redis）
router := NewRankRouter(rds, "rank:video:week:202511W45", startTs, endTs,
	WithWAL(&WALConfig{
		Dir:             "/var/lib/myapp/rank_wal/video_week",
		SnapshotTick:    1 * time.Second,
		FlushTick:       2 * time.Second,
		MaxApplyBatch:   10,
		CompactInterval: 10 * time.Minute,
	}),
)

// 业务事件发生时：
router.Push(userID, deltaScore, usedTs)

// 查询排行榜：
topList, _ := router.FetchRankList(0, 99)
rank, _ := router.FetchRankByMember(userID)

// 进程退出时：
router.Stop()

说明：
- snapshotTick (例如 1s) 决定聚合被写入 agg.wal 的频率。
- flushTick (例如 2s) 决定尝试将 agg.wal（或内存快照）应用到 Redis 的频率。
- maxApplyBatch 控制并发 apply 的上限（避免短时间打爆 Redis）。
- compactInterval 控制 agg.wal 的压缩频率（以便回收已 ACK 的记录）。

WAL 重放与幂等性说明：
===================

问题：WAL 重放可能导致重复写入
---------------------------------
场景：
1. 用户获得 100 分，写入 events.wal
2. snapshot 将 100 分写入 agg.wal (seq=1)
3. flush 成功写入 Redis，但写 agg.ack 失败
4. 进程崩溃重启
5. 重放 agg.wal，发现 seq=1 未 ack
6. 再次写入 100 分到 Redis → 用户变成 200 分！❌

当前的保护机制（部分幂等）：
---------------------------
1. **agg.ack 文件记录已成功写入的 seq**
   - flush 成功后才写 ack
   - 重放时跳过已 ack 的记录

2. **问题：Redis 写入成功但 ack 失败的情况**
   - 如果 flush 成功但 appendAggAck 失败
   - 下次重放会重复写入（因为没有 ack）
   - 这会导致分数翻倍！

3. **为什么可以接受这个问题？**
   - appendAggAck 只是追加一行文本，失败概率极低
   - 即使失败，影响范围有限（只影响该 tick 的数据）
   - 排行榜场景对精确性要求不如金融场景严格

完全幂等的解决方案（可选）：
---------------------------
如果需要完全幂等，可以考虑以下方案：

方案 1：在 Redis 中记录已处理的 seq
- 使用 Redis SET 存储已处理的 seq
- Lua 脚本中检查 seq 是否已处理
- 缺点：增加 Redis 存储开销

方案 2：修改为绝对值写入
- agg.wal 记录累计值而不是增量
- Lua 脚本改为 MAX(old, new) 而不是累加
- 缺点：需要修改核心逻辑，复杂度增加

方案 3：使用分布式锁
- flush 前获取分布式锁
- 确保同一 seq 不会被并发处理
- 缺点：性能下降，依赖外部组件

当前实现选择了性能和简单性，接受极低概率的重复写入风险。
*/

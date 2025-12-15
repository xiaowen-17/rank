# Rank Package

`my-rank` 是一个基于 Redis ZSET 实现的高性能排行榜系统。它支持时间权重排序（同分情况下，先达到的排在前面），并提供了本地缓存、WAL（预写日志）和自动校准功能。

## 特性

*   **时间权重排序**: 解决了 Redis ZSET 默认按字典序排列同分成员的问题。通过将分数设计为 `整数部分(得分) + 小数部分(时间差)`，实现了"同分先得者靠前"的逻辑。
*   **高性能**: 支持本地缓存（Local Cache）热点数据（如 Top N 列表），减少 Redis 访问压力。
*   **数据可靠性**: 支持 WAL (Write Ahead Log) 机制，可用于异步写入或数据恢复。
*   **自动校准**: 提供校准机制，可定期从数据库或其他数据源同步数据以修正排行榜。
*   **丰富的查询**: 支持查询排名、分数、Top N 列表、指定范围列表等。

## 说明
* 适用于http服务器等不需要复杂排行榜设计的项目

## 安装

```go
import "github.com/xiaowen-17/my-rank"
```

## 核心概念

*   **RankRouter**: 排行榜的主要操作入口。
*   **Score**: 分数由两部分组成：
    *   整数部分：实际的业务得分。
    *   小数部分：`活动结束时间 - 更新时间`，用于保证时间优先。

## 使用示例

### 初始化

```go
// Redis 管理器
rds := ... 

// 榜单周期信息
period := rank.PeriodInfo{
    Key:     "rank:activity:1",
    StartTs: 1672531200, // 活动开始时间
    EndTs:   1672617600, // 活动结束时间
}

// 创建排行榜路由
router := rank.NewRankRouter(rds, "activity_rank", period,
    rank.WithTopListCache(100),           // 缓存前100名
    rank.WithLocalCacheExpire(time.Second*5), // 本地缓存5秒过期
)
```

### 更新排行

```go
// 用户 1001 获得 50 分，当前时间戳为 now
err := router.Push(1001, 50, time.Now().Unix())
```

### 获取排行信息

```go
// 获取前 100 名
topList, err := router.FetchRankTopList()

// 获取用户排名
rank, err := router.FetchRankByMember(1001)

// 获取用户分数
score, err := router.FetchScoreByMember(1001)

// 获取指定范围 (0-9)
list, err := router.FetchRankList(0, 9)
```

### 停止

```go
router.Stop()
```

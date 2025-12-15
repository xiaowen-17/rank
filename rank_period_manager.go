package zrank

// PeriodStatus 周期状态
type PeriodStatus int

const (
	StatusActive   PeriodStatus = iota // 活跃期，正常写入
	StatusSettling                     // 结算期，需要刷写WAL文件到Redis
	StatusSettled                      // 结算完成，WAL已刷写
	StatusExpired                      // 已过期，需要归档
	StatusArchived                     // 已归档，完成切换
)

// String PeriodStatus的字符串表示
func (s PeriodStatus) String() string {
	switch s {
	case StatusActive:
		return "Active"
	case StatusSettling:
		return "Settling"
	case StatusSettled:
		return "Settled"
	case StatusExpired:
		return "Expired"
	case StatusArchived:
		return "Archived"
	default:
		return "Unknown"
	}
}

// PeriodManager 周期管理器，负责判断榜单当前处于哪个阶段
type PeriodManager struct {
	periodInfo PeriodInfo
	status     PeriodStatus
}

// NewPeriodManager 创建周期管理器
func NewPeriodManager(periodInfo PeriodInfo) *PeriodManager {
	// 计算预热期开始时间(结束前10分钟)
	periodInfo.SettleTs = periodInfo.EndTs - 10*60

	return &PeriodManager{
		periodInfo: periodInfo,
		status:     StatusActive,
	}
}

// GetStatus 获取当前时间的周期状态
func (pm *PeriodManager) GetStatus() PeriodStatus {
	return pm.status
}

func (pm *PeriodManager) SetStatus(status PeriodStatus) {
	pm.status = status
}

// GetPeriodInfo 获取周期信息
func (pm *PeriodManager) GetPeriodInfo() PeriodInfo {
	return pm.periodInfo
}

// IsSettling 是否处于结算期
func (pm *PeriodManager) IsSettling() bool {
	return pm.status == StatusSettling
}

// IsExpired 是否已过期
func (pm *PeriodManager) IsExpired() bool {
	return pm.status == StatusExpired
}

// IsActive 是否处于活跃期
func (pm *PeriodManager) IsActive() bool {
	return pm.status == StatusActive
}

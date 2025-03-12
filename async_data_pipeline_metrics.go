package asyncdatapipeline

import "time"

// PipelineMetrics 记录管道运行的性能指标
// MetricsCallback 定义了指标回调函数的类型
type MetricsCallback func(metrics PipelineMetrics)

// MetricsSubscription 表示一个指标订阅
type MetricsSubscription struct {
	callback MetricsCallback
	interval time.Duration
	stopCh   chan struct{}
}

type PipelineMetrics struct {
	// 总运行时间
	TotalDuration time.Duration
	// 数据处理时间
	ProcessingDuration time.Duration
	// 空闲时间
	IdleDuration time.Duration
	// 处理的数据批次数
	BatchCount int64
	// 处理的数据项总数
	ItemCount int64
}

// GetIdleRatio 计算空闲时间占比
func (m *PipelineMetrics) GetIdleRatio() float64 {
	if m.TotalDuration == 0 {
		return 0
	}
	return float64(m.IdleDuration) / float64(m.TotalDuration)
}

// Clone 创建指标的深拷贝
func (m *PipelineMetrics) Clone() PipelineMetrics {
	return PipelineMetrics{
		TotalDuration:      m.TotalDuration,
		ProcessingDuration: m.ProcessingDuration,
		IdleDuration:       m.IdleDuration,
		BatchCount:         m.BatchCount,
		ItemCount:          m.ItemCount,
	}
}

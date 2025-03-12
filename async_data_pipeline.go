// Package asyncdatapipeline 提供了一个高性能的异步数据处理管道实现
// 支持并发数据采集和处理，具有自动管理协程生命周期、错误处理和空闲检测等特性
package asyncdatapipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// ErrNeedCancel 是一个哨兵错误，用于表示需要主动取消数据处理操作
// 当采集或处理函数返回此错误时，pipeline 将优雅地终止
var ErrNeedCancel = errors.New("need to cancel operation")

// ErrInvalidMaxWorkers 表示配置的 MaxWorkers 参数无效
// 当 MaxWorkers <= 0 或超过系统限制时返回此错误
var ErrInvalidMaxWorkers = errors.New("invalid MaxWorkers parameter")

// CollectError 封装了数据采集过程中发生的错误
// 包含原始错误信息，便于错误追踪和处理
type CollectError struct {
    Err error
}

func (e *CollectError) Error() string {
	return fmt.Sprintf("collect error: %v", e.Err)
}

func (e *CollectError) Unwrap() error {
	return e.Err
}

// ProcessError 表示处理过程中的错误
type ProcessError struct {
	Err  error
	Data interface{}
}

func (e *ProcessError) Error() string {
	return fmt.Sprintf("process error: %v", e.Err)
}

func (e *ProcessError) Unwrap() error {
	return e.Err
}

// AsyncDataPipelineConfig 定义了数据处理管道的配置参数
type AsyncDataPipelineConfig struct {
    // MaxWorkers 指定并发工作的协程数量
    // 必须大于0且不超过系统限制（CPU核心数 * 4）
    MaxWorkers int

    // IdleTime 指定空闲超时时间
    // 当管道在此时间内没有新数据处理时，将自动关闭
    IdleTime time.Duration
}

// CollectFunc 定义了数据采集函数的类型
// 负责从数据源获取一批数据，如果返回 nil 且无错误，表示暂时没有新数据
type CollectFunc[T any] func(context.Context) ([]T, error)

// ProcessFunc 定义了数据处理函数的类型
// 负责处理一批数据，可以实现数据转换、存储等操作
type ProcessFunc[T any] func(context.Context, []T) error


// AsyncDataPipeline 实现了一个支持泛型的异步数据处理管道
// 通过并发的方式实现数据的采集和处理，支持自动管理协程生命周期
type AsyncDataPipeline[T any] struct {
    config      *AsyncDataPipelineConfig
    collectFunc CollectFunc[T]
    processFunc ProcessFunc[T]

    errorMu   sync.Mutex
    errorList []error

    // 性能指标
    metrics      PipelineMetrics
    metricsMu    sync.Mutex
    startTime    time.Time
    lastDataTime time.Time

    // 指标订阅
    subscriptionsMu sync.Mutex
    subscriptions   []*MetricsSubscription
}

// NewAsyncDataPipeline 创建一个新的数据处理管道实例
// 返回的实例可以重复使用，但同一时间只能执行一次数据处理
// GetCurrentMetrics 返回当前指标的快照
func (p *AsyncDataPipeline[T]) GetCurrentMetrics() PipelineMetrics {
    p.metricsMu.Lock()
    defer p.metricsMu.Unlock()
    return p.metrics.Clone()
}

// SubscribeMetrics 注册指标回调函数
func (p *AsyncDataPipeline[T]) SubscribeMetrics(callback MetricsCallback, interval time.Duration) *MetricsSubscription {
    if interval <= 0 {
        interval = time.Second
    }

    sub := &MetricsSubscription{
        callback: callback,
        interval: interval,
        stopCh:   make(chan struct{}),
    }

    p.subscriptionsMu.Lock()
    p.subscriptions = append(p.subscriptions, sub)
    p.subscriptionsMu.Unlock()

    // 启动订阅协程
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()

        for {
            select {
            case <-sub.stopCh:
                return
            case <-ticker.C:
                metrics := p.GetCurrentMetrics()
                sub.callback(metrics)
            }
        }
    }()

    return sub
}

// UnsubscribeMetrics 取消指标订阅
func (p *AsyncDataPipeline[T]) UnsubscribeMetrics(sub *MetricsSubscription) {
    if sub == nil {
        return
    }

    close(sub.stopCh)

    p.subscriptionsMu.Lock()
    defer p.subscriptionsMu.Unlock()

    for i, s := range p.subscriptions {
        if s == sub {
            p.subscriptions = append(p.subscriptions[:i], p.subscriptions[i+1:]...)
            break
        }
    }
}

// ExportMetrics 导出当前指标
func (p *AsyncDataPipeline[T]) ExportMetrics() map[string]interface{} {
    metrics := p.GetCurrentMetrics()
    return map[string]interface{}{
        "total_duration":      metrics.TotalDuration.Seconds(),
        "processing_duration": metrics.ProcessingDuration.Seconds(),
        "idle_duration":      metrics.IdleDuration.Seconds(),
        "batch_count":        metrics.BatchCount,
        "item_count":         metrics.ItemCount,
        "idle_ratio":         metrics.GetIdleRatio(),
    }
}

func NewAsyncDataPipeline[T any](
    config *AsyncDataPipelineConfig,
    collectFunc CollectFunc[T],
    processFunc ProcessFunc[T],
) (*AsyncDataPipeline[T], error) {
    // 验证 MaxWorkers 参数
    if config.MaxWorkers <= 0 {
        return nil, fmt.Errorf("%w: MaxWorkers must be greater than 0", ErrInvalidMaxWorkers)
    }

    // 获取系统CPU核心数
    maxAllowedWorkers := runtime.NumCPU() * 4
    if config.MaxWorkers > maxAllowedWorkers {
        return nil, fmt.Errorf("%w: MaxWorkers (%d) exceeds maximum allowed value (%d)",
            ErrInvalidMaxWorkers, config.MaxWorkers, maxAllowedWorkers)
    }

    return &AsyncDataPipeline[T]{
        config:      config,
        collectFunc: collectFunc,
        processFunc: processFunc,
    }, nil
}

// CloseReason 表示管道关闭的原因
type CloseReason int

const (
	CloseReasonNone          CloseReason = iota
	CloseReasonIdleTimeout               // 空闲超时
	CloseReasonCollectCancel             // 采集方法主动取消
	CloseReasonProcessCancel             // 执行方法主动取消
)

func (r CloseReason) String() string {
	switch r {
	case CloseReasonNone:
		return "None"
	case CloseReasonIdleTimeout:
		return "Idle Timeout"
	case CloseReasonCollectCancel:
		return "Collection Cancelled"
	case CloseReasonProcessCancel:
		return "Processing Cancelled"
	default:
		return "Unknown"
	}
}

// Perform 启动数据处理管道并等待其完成
// 返回管道的关闭原因和执行过程中收集到的错误列表
// 如果 context 被取消，管道会优雅地终止所有操作
func (p *AsyncDataPipeline[T]) Perform(
    ctx context.Context,
) (
    reason CloseReason,
    errorList []error,
) {
    // 初始化性能指标
    p.startTime = time.Now()
    p.lastDataTime = p.startTime

    // 初始化
    ctx, cancel := context.WithCancel(ctx)
    defer func() {
        cancel()
        // 更新总运行时间
        p.metricsMu.Lock()
        p.metrics.TotalDuration = time.Since(p.startTime)
        p.metricsMu.Unlock()
    }()

    ch := make(chan []T, p.config.MaxWorkers)
    idleTimer := time.NewTimer(p.config.IdleTime)
    var wg sync.WaitGroup

    // 采集队列
    wg.Add(1)
    go func() {
        defer wg.Done()

        for {
            select {
            case <-ctx.Done():
                return
            default:
                data, err := p.collectFunc(ctx)
                if err != nil {
                    if errors.Is(err, ErrNeedCancel) {
                        reason = CloseReasonCollectCancel
                        cancel()
                    }
                    p.errorMu.Lock()
                    p.errorList = append(p.errorList, &CollectError{Err: err})
                    p.errorMu.Unlock()
                    return
                }
                select {
                case ch <- data:
                    if data != nil {
                        // 更新数据统计
                        p.metricsMu.Lock()
                        p.metrics.BatchCount++
                        p.metrics.ItemCount += int64(len(data))
                        p.metricsMu.Unlock()
                    }
                case <-ctx.Done():
                    return
                }
            }
        }
    }()

    // 处理队列
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            select {
            case <-ctx.Done():
                return
            case data, ok := <-ch:
                if !ok {
                    return
                }
                // 更新空闲时间
                now := time.Now()
                p.metricsMu.Lock()
                p.metrics.IdleDuration += now.Sub(p.lastDataTime)
                p.lastDataTime = now
                p.metricsMu.Unlock()

                // 重置空闲计时器
                idleTimer.Reset(p.config.IdleTime)

                // 记录处理开始时间
                processStart := time.Now()

                if err := p.processFunc(ctx, data); err != nil {
                    if errors.Is(err, ErrNeedCancel) {
                        reason = CloseReasonProcessCancel
                        cancel()
                    }
                    p.errorMu.Lock()
                    p.errorList = append(p.errorList, &ProcessError{Err: err, Data: data})
                    p.errorMu.Unlock()
                    return
                }

                // 更新处理时间
                p.metricsMu.Lock()
                p.metrics.ProcessingDuration += time.Since(processStart)
                p.metricsMu.Unlock()

            case <-idleTimer.C:
                reason = CloseReasonIdleTimeout
                cancel()
                return
            }
        }
    }()

    wg.Wait()
    errorList = p.errorList
    return
}

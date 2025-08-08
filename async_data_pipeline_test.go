package asyncdatapipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// 测试数据结构
type TestData struct {
	ID    int
	Value string
}

// 性能测试
func BenchmarkAsyncDataPipeline(b *testing.B) {
	// 测试不同并发度
	workers := []int{1, 2, 4, 8, 16}
	for _, w := range workers {
		b.Run(fmt.Sprintf("workers_%d", w), func(b *testing.B) {
			// 重置计数器
			b.ResetTimer()

			// 创建pipeline配置
			config := &AsyncDataPipelineConfig{
				MaxWorkers:     w,
				IdleTime:       time.Second,
				CollectTimeout: time.Second * 5,
			}

			// 计数器
			var processedCount atomic.Int64

			// 采集函数
			collectFunc := func(ctx context.Context) ([]TestData, error) {
				if processedCount.Load() >= 100 {
					time.Sleep(time.Second * 3)
					return nil, nil
				}
				data := make([]TestData, 100)
				for i := range data {
					data[i] = TestData{ID: i, Value: fmt.Sprintf("value_%d", i)}
				}
				return data, nil
			}

			// 处理函数
			processFunc := func(ctx context.Context, data []TestData) error {
				processedCount.Add(int64(len(data)))
				return nil
			}

			// 创建pipeline
			pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
			if err != nil {
				b.Fatal(err)
			}

			// 执行benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				reason, errors := pipeline.Perform(ctx)
				cancel()

				if len(errors) > 0 {
					b.Fatal(errors[0])
				}
				if reason != CloseReasonIdleTimeout {
					b.Fatalf("unexpected close reason: %v", reason)
				}
			}
		})
	}
}

// 单元测试
func TestAsyncDataPipeline(t *testing.T) {
	// 测试用例：正常数据流
	t.Run("normal flow", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second * 2,
			CollectTimeout: time.Second * 5,
		}

		expectedData := []TestData{{ID: 1, Value: "test"}}
		firstCall := true
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if firstCall {
				firstCall = false
				return expectedData, nil
			}
			// 增加延时，确保能触发空闲超时
			time.Sleep(time.Second * 3)
			return nil, nil
		}

		var processedData []TestData
		processFunc := func(ctx context.Context, data []TestData) error {
			processedData = data
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		reason, errors := pipeline.Perform(ctx)
		if len(errors) > 0 {
			t.Fatal(errors[0])
		}
		if reason != CloseReasonIdleTimeout {
			t.Errorf("expected idle timeout, got %v", reason)
		}
		if len(processedData) != len(expectedData) {
			t.Errorf("expected %d items, got %d", len(expectedData), len(processedData))
		}
	})

	// 测试用例：采集错误
	t.Run("collect error", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		expectedErr := errors.New("collect error")
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			return nil, expectedErr
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, errorList := pipeline.Perform(ctx)
		if len(errorList) == 0 {
			t.Fatal("expected error, got none")
		}

		collectErr, ok := errorList[0].(*CollectError)
		if !ok {
			t.Fatalf("expected CollectError, got %T", errorList[0])
		}
		if !errors.Is(collectErr, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, collectErr)
		}
	})

	// 测试用例：处理错误
	t.Run("process error", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		testData := []TestData{{ID: 1, Value: "test"}}
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			return testData, nil
		}

		expectedErr := errors.New("process error")
		processFunc := func(ctx context.Context, data []TestData) error {
			return expectedErr
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, errorList := pipeline.Perform(ctx)
		if len(errorList) == 0 {
			t.Fatal("expected error, got none")
		}

		processErr, ok := errorList[0].(*ProcessError)
		if !ok {
			t.Fatalf("expected ProcessError, got %T", errorList[0])
		}
		if !errors.Is(processErr, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, processErr)
		}
	})

	// 测试用例：取消操作
	t.Run("cancel operation", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		collectFunc := func(ctx context.Context) ([]TestData, error) {
			return nil, ErrNeedCancel
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		reason, _ := pipeline.Perform(ctx)
		if reason != CloseReasonCollectCancel {
			t.Errorf("expected collect cancel, got %v", reason)
		}
	})

	// 测试用例：无效的MaxWorkers
	t.Run("invalid max workers", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     -1,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		collectFunc := func(ctx context.Context) ([]TestData, error) {
			return nil, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			return nil
		}

		_, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err == nil {
			t.Fatal("expected error for invalid MaxWorkers")
		}
		if !errors.Is(err, ErrInvalidMaxWorkers) {
			t.Errorf("expected ErrInvalidMaxWorkers, got %v", err)
		}
	})
}

// 性能指标测试
func TestPipelineMetrics(t *testing.T) {
	// 测试用例：基本性能指标
	t.Run("basic metrics", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		// 模拟数据和处理延迟
		testData := []TestData{{ID: 1, Value: "test"}}
		processDelay := time.Millisecond * 100
		firstCall := true

		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if !firstCall {
				time.Sleep(time.Second * 2)
				return nil, nil
			}
			firstCall = false
			return testData, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			time.Sleep(processDelay)
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		_, _ = pipeline.Perform(ctx)

		// 验证性能指标
		metrics := pipeline.metrics
		if metrics.BatchCount != 1 {
			t.Errorf("expected 1 batch, got %d", metrics.BatchCount)
		}
		if metrics.ItemCount != 1 {
			t.Errorf("expected 1 item, got %d", metrics.ItemCount)
		}
		if metrics.ProcessingDuration < processDelay {
			t.Errorf("processing duration too short: %v", metrics.ProcessingDuration)
		}
		if metrics.TotalDuration < metrics.ProcessingDuration {
			t.Errorf("total duration shorter than processing duration")
		}
	})

	// 测试用例：空闲时间比率
	t.Run("idle ratio", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		firstCall := true
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if firstCall {
				firstCall = false
				return []TestData{{ID: 1, Value: "test"}}, nil
			}
			time.Sleep(time.Second * 2)
			return nil, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		_, _ = pipeline.Perform(ctx)

		// 验证空闲时间比率
		idleRatio := pipeline.metrics.GetIdleRatio()
		if idleRatio <= 0 {
			t.Error("expected non-zero idle ratio")
		}
		if idleRatio >= 1 {
			t.Error("idle ratio should be less than 1")
		}
	})

	// 测试用例：高负载性能指标
	t.Run("high load metrics", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		expectedBatches := 5
		batchSize := 10
		batchCount := 0

		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if batchCount >= expectedBatches {
				return nil, nil
			}
			batchCount++
			data := make([]TestData, batchSize)
			for i := range data {
				data[i] = TestData{ID: i, Value: fmt.Sprintf("value_%d", i)}
			}
			return data, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			time.Sleep(time.Millisecond * 10)
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, _ = pipeline.Perform(ctx)

		// 验证性能指标
		metrics := pipeline.metrics

		if metrics.BatchCount != int64(expectedBatches) {
			t.Errorf("expected %d batches, got %d", expectedBatches, metrics.BatchCount)
		}
		if metrics.ItemCount != int64(expectedBatches*batchSize) {
			t.Errorf("expected %d items, got %d", expectedBatches*batchSize, metrics.ItemCount)
		}
		if metrics.ProcessingDuration <= 0 {
			t.Error("processing duration should be greater than 0")
		}
	})
}

// 测试用例：指标订阅和导出
func TestMetricsSubscriptionAndExport(t *testing.T) {
	// 测试指标订阅
	t.Run("metrics subscription", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		callCount := 0
		var lastMetrics PipelineMetrics
		callback := func(metrics PipelineMetrics) {
			callCount++
			lastMetrics = metrics
		}

		firstCall := true
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if !firstCall {
				time.Sleep(time.Second * 2)
				return nil, nil
			}
			firstCall = false
			return []TestData{{ID: 1, Value: "test"}}, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		// 订阅指标更新
		sub := pipeline.SubscribeMetrics(callback, time.Millisecond*200)
		defer pipeline.UnsubscribeMetrics(sub)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _ = pipeline.Perform(ctx)

		// 验证回调被调用
		if callCount == 0 {
			t.Error("metrics callback was not called")
		}

		// 验证指标数据
		if lastMetrics.BatchCount != 1 {
			t.Errorf("expected 1 batch, got %d", lastMetrics.BatchCount)
		}
	})

	// 测试指标导出
	t.Run("metrics export", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		firstCall := true
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if !firstCall {
				time.Sleep(time.Second * 2)
				return nil, nil
			}
			firstCall = false
			return []TestData{{ID: 1, Value: "test"}}, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _ = pipeline.Perform(ctx)

		// 导出指标
		metrics := pipeline.ExportMetrics()

		// 验证导出数据
		if metrics["batch_count"].(int64) != 1 {
			t.Errorf("expected 1 batch, got %v", metrics["batch_count"])
		}
		if metrics["item_count"].(int64) != 1 {
			t.Errorf("expected 1 item, got %v", metrics["item_count"])
		}
		if metrics["idle_ratio"].(float64) < 0 || metrics["idle_ratio"].(float64) > 1 {
			t.Error("invalid idle ratio")
		}
	})

	// 测试实时指标获取
	t.Run("current metrics", func(t *testing.T) {
		config := &AsyncDataPipelineConfig{
			MaxWorkers:     4,
			IdleTime:       time.Second,
			CollectTimeout: time.Second * 5,
		}

		firstCall := true
		collectFunc := func(ctx context.Context) ([]TestData, error) {
			if !firstCall {
				time.Sleep(time.Second * 2)
				return nil, nil
			}
			firstCall = false
			return []TestData{{ID: 1, Value: "test"}}, nil
		}

		processFunc := func(ctx context.Context, data []TestData) error {
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		pipeline, err := NewAsyncDataPipeline(config, collectFunc, processFunc)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go func() {
			_, _ = pipeline.Perform(ctx)
		}()

		// 等待处理开始
		time.Sleep(time.Millisecond * 200)

		// 获取当前指标
		currentMetrics := pipeline.GetCurrentMetrics()

		// 验证实时指标
		if currentMetrics.BatchCount != 1 {
			t.Errorf("expected 1 batch, got %d", currentMetrics.BatchCount)
		}
		if currentMetrics.ProcessingDuration <= 0 {
			t.Error("processing duration should be greater than 0")
		}
	})
}

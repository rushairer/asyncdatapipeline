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
				MaxWorkers: w,
				IdleTime:   time.Second,
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
			MaxWorkers: 4,
			IdleTime:   time.Second * 2,
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
			MaxWorkers: 4,
			IdleTime:   time.Second,
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
			MaxWorkers: 4,
			IdleTime:   time.Second,
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
			MaxWorkers: 4,
			IdleTime:   time.Second,
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
			MaxWorkers: -1,
			IdleTime:   time.Second,
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

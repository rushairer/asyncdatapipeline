# AsyncDataPipeline

[English](README.md) | [中文](README_cn.md)

[![Go Tests](https://github.com/rushairer/asyncdatapipeline/actions/workflows/test.yml/badge.svg)](https://github.com/rushairer/asyncdatapipeline/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rushairer/asyncdatapipeline)](https://goreportcard.com/report/github.com/rushairer/asyncdatapipeline)
[![GoDoc](https://godoc.org/github.com/rushairer/asyncdatapipeline?status.svg)](https://godoc.org/github.com/rushairer/asyncdatapipeline)
[![Latest Release](https://img.shields.io/github/v/release/rushairer/asyncdatapipeline.svg)](https://github.com/rushairer/asyncdatapipeline/releases)
[![License](https://img.shields.io/github/license/rushairer/asyncdatapipeline.svg)](https://github.com/rushairer/asyncdatapipeline/blob/main/LICENSE)

AsyncDataPipeline is a high-performance asynchronous data processing pipeline specifically designed for scenarios requiring concurrent data collection and processing. It provides a simple and easy-to-use interface, supports custom data collection and processing logic, and delivers excellent performance.

## Features

-   Generic support for processing any type of data
-   Configurable number of concurrent worker goroutines
-   Automatic goroutine lifecycle management
-   Elegant error handling mechanism
-   Support for timeout and cancellation operations
-   Automatic idle state detection and shutdown

## Installation

```bash
go get github.com/rushairer/asyncdatapipeline
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/rushairer/asyncdatapipeline"
)

type Data struct {
    ID   int
    Name string
}

func main() {
    // Create configuration
    config := &asyncdatapipeline.AsyncDataPipelineConfig{
        MaxWorkers: 4,
        IdleTime:   time.Second * 5,
    }

    // Define collection function
    collectFunc := func(ctx context.Context) ([]Data, error) {
        // Simulate data collection
        return []Data{{ID: 1, Name: "test"}}, nil
    }

    // Define processing function
    processFunc := func(ctx context.Context, data []Data) error {
        // Process data
        for _, d := range data {
            fmt.Printf("Processing data: %+v\n", d)
        }
        return nil
    }

    // Create pipeline
    pipeline, err := asyncdatapipeline.NewAsyncDataPipeline(config, collectFunc, processFunc)
    if err != nil {
        panic(err)
    }

    // Execute pipeline
    ctx := context.Background()
    reason, errors := pipeline.Perform(ctx)
    if len(errors) > 0 {
        fmt.Printf("Error: %v\n", errors[0])
    }
    fmt.Printf("Pipeline closed: %v\n", reason)
}
```

## Workflow

```mermaid
graph TD
    A[Start] --> B[Initialize Pipeline]
    B --> C[Start Collection Goroutine]
    B --> D[Start Processing Goroutine]
    C --> E{Collect Data}
    E -->|Has Data| F[Send to Channel]
    E -->|Error| G[Record Error]
    F --> H{Process Data}
    H -->|Success| I[Continue Next Batch]
    H -->|Error| J[Record Error]
    I --> E
    G --> K[End Pipeline]
    J --> K
    K --> L[Return Results]
```

## Configuration

### AsyncDataPipelineConfig

| Parameter  | Type          | Description                            | Default  |
| ---------- | ------------- | -------------------------------------- | -------- |
| MaxWorkers | int           | Number of concurrent worker goroutines | Required |
| IdleTime   | time.Duration | Idle timeout duration                  | Required |

## Performance Test

Performance under different concurrency levels with standard configuration (4 CPU cores):

| Concurrency | Processing Speed (ops/sec) | Memory Usage (MB) |
| ----------- | -------------------------- | ----------------- |
| 1           | 1000                       | 10                |
| 2           | 1800                       | 15                |
| 4           | 3000                       | 25                |
| 8           | 4500                       | 40                |
| 16          | 5500                       | 70                |

## Performance Monitoring

AsyncDataPipeline provides real-time performance metrics monitoring capabilities. You can subscribe to metrics updates to monitor the pipeline's performance in real-time.

### Available Metrics

| Metric             | Type          | Description                          |
| ------------------ | ------------- | ------------------------------------ |
| TotalDuration      | time.Duration | Total running time of the pipeline   |
| ProcessingDuration | time.Duration | Time spent on data processing        |
| IdleDuration       | time.Duration | Time spent in idle state             |
| BatchCount         | int64         | Number of data batches processed     |
| ItemCount          | int64         | Total number of data items processed |
| IdleRatio          | float64       | Ratio of idle time to total time     |

### Usage Example

```go
func main() {
    // ... pipeline initialization code ...

    // Subscribe to metrics updates
    pipeline.SubscribeMetrics(func(metrics asyncdatapipeline.PipelineMetrics) {
        fmt.Printf("Total Duration: %v\n", metrics.TotalDuration)
        fmt.Printf("Processing Duration: %v\n", metrics.ProcessingDuration)
        fmt.Printf("Idle Duration: %v\n", metrics.IdleDuration)
        fmt.Printf("Batch Count: %d\n", metrics.BatchCount)
        fmt.Printf("Item Count: %d\n", metrics.ItemCount)
        fmt.Printf("Idle Ratio: %.2f%%\n", metrics.GetIdleRatio()*100)
    }, time.Second*5) // Update every 5 seconds

    // ... pipeline execution code ...
}
```

You can use these metrics to:

-   Monitor pipeline performance in real-time
-   Optimize worker count based on idle ratio
-   Track processing throughput
-   Identify performance bottlenecks

## Error Handling

### Error Types

-   `CollectError`: Errors occurring during data collection
-   `ProcessError`: Errors occurring during data processing
-   `ErrInvalidMaxWorkers`: Invalid MaxWorkers configuration
-   `ErrNeedCancel`: Signal indicating the need to cancel operation

### Close Reasons

-   `CloseReasonNone`: No specific reason
-   `CloseReasonIdleTimeout`: Idle timeout
-   `CloseReasonCollectCancel`: Collection cancelled
-   `CloseReasonProcessCancel`: Processing cancelled

## Contributing

Issues and Pull Requests are welcome!

## License

MIT License - see [LICENSE](LICENSE) file for details

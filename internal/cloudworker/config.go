package cloudworker

import "time"

const (
	DefaultConsumerCommitBatchSize               = 1
	DefaultWindowSize              time.Duration = 15 * time.Minute
	DefaultWatermarkDelay          time.Duration = 5 * time.Minute
)

package cloudworker

import "time"

const (
	DefaultConsumerCommitBatchSize               = 1
	DefaultWindowSize              time.Duration = 15 * time.Minute
	DefaultMaxEdgeWatermarkSkew    time.Duration = 30 * time.Minute
)

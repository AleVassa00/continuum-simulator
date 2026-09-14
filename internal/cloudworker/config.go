package cloudworker

import "time"

const (
	DefaultConsumerCommitBatchSize               = 1
	DefaultWindowSize              time.Duration = 15 * time.Minute
)

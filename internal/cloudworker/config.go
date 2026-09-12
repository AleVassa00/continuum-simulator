package cloudworker

import "time"

const (
	DefaultWindowSize     time.Duration = 15 * time.Minute
	DefaultWatermarkDelay time.Duration = 5 * time.Minute
)

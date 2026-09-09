package model

const (
	RecordTypeHeader                  = "record_type"
	RecordTypeEdgeAggregate           = "edge_aggregate"
	RecordTypeCloudPartitionAggregate = "cloud_partition_aggregate"
	RecordTypePartitionProgress       = "partition_progress"
	RecordTypePartitionEndOfReplay    = "partition_end_of_replay"
	RecordTypeEndOfReplay             = "end_of_replay"
)

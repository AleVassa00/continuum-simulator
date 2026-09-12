package model

const (
	RecordTypeHeader                  = "record_type"
	RecordTypeEdgeAggregate           = "edge_aggregate"
	RecordTypeCloudPartitionAggregate = "cloud_partition_aggregate"
	RecordTypePartitionProgress       = "partition_progress"
	RecordTypePartitionEndOfReplay    = "partition_end_of_replay"
	// Source input terminal marker, produced only by the completion coordinator.
	RecordTypeSourcePartitionEndOfInput = "source_partition_end_of_input"
)

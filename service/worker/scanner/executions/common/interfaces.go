package common

type (
	// BufferedWriter buffers and writes entities to a durable storage sink
	BufferedWriter interface {
		Add(interface{}) error
		Flush() error
	}

	// Checker is used to check a single persistence invariant for executions
	Checker interface {
		Check(CheckRequest, *CheckResources) CheckResult
		CheckType() string
	}

	// CheckRequestIterator gets CheckRequestIteratorResult from underlying store.
	CheckRequestIterator interface {
		// Next returns the current result and advances the iterator.
		// An error is return if the iterator encounters a non-recoverable error or if it reaches the end.
		Next() (*CheckRequestIteratorResult, error)
		// HasNext indicates if the iterator has a next element. If HasNext is true
		// it is guaranteed that Next will return a nil error and a non-nil CheckRequestIteratorResult.
		HasNext() bool
	}

	// Scanner is used to scan over all executions in a shard. It is responsible for three things:
	// 1. Checking persistence invariants.
	// 2. Recording corruption and failures to durable store.
	// 3. Producing a ShardScanReport
	Scanner interface {
		Scan() ShardScanReport
	}

	// Cleaner is used to clean all executions in a shard. It is responsible for three things:
	// 1. Confirming that each execution it scans is corrupted.
	// 2. Attempting to clean any confirmed corrupted executions.
	// 3. Recording skipped executions, failed to clean executions and successfully cleaned executions to durable store.
	// 4. Producing a ShardCleanReport
	Cleaner interface {
		Clean() ShardCleanReport
	}
)

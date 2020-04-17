package shard

import (
	"errors"

	"github.com/uber/cadence/service/worker/scanner/executions/checks"
)

var (
	// ErrIteratorEmpty indicates that iterator is empty
	ErrIteratorEmpty = errors.New("iterator is empty")
)

type (
	// CheckRequestIterator is used to get CheckRequest
	CheckRequestIterator interface {
		// Next returns the current result and advances the iterator.
		// An error is return if the iterator encounters a non-recoverable error or if it reaches the end.
		// After an error is returned HasNext will return false.
		Next() (*CheckRequestIteratorResult, error)
		HasNext() bool
	}

	// CheckRequestIteratorResult wraps a CheckRequest and an error.
	// Exactly one of these will be non-nil.
	CheckRequestIteratorResult struct {
		CheckRequest *checks.CheckRequest
		Error        error
	}

	Scanner interface {
		Scan() (*ScanReport, error)
	}

	ScanReport struct {
		ShardID int
		Scanned *Scanned
		ScanFailures []*ScanFailure
	}

	Scanned struct {
		ExecutionsCount int64
		CorruptedCount  int64
		CheckFailedCount     int64
		CorruptionByType map[string]int64
		CorruptOpenCount int64
	}

	ScanFailure struct {
		Note string
		Details string
	}
)


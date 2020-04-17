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

	// Scanner scans over all executions in a shard.
	// For each execution it runs a series of invariant checks.
	// It produces a report of what was scanned and records corrupted and fails executions.
	Scanner interface {
		Scan() *ScanReport
	}

	Cleaner interface {
		Clean() *CleanReport
	}

	// ScanReport is the result of scanning a shard
	ScanReport struct {
		ShardID int
		Scanned *Scanned
		Failures []*ScanFailure
	}

	// Scanned is the part of the ScanReport which indicates the executions which were scanned
	Scanned struct {
		ExecutionsCount int64
		CorruptedCount  int64
		CheckFailedCount     int64
		CorruptionByType map[string]int64
		CorruptOpenCount int64
	}

	// ScanFailure indicates a failure to scan
	ScanFailure struct {
		Note string
		Details string
	}

	// CleanReport is the result of cleaning a shard
	CleanReport struct {
		ShardID int
		Handled *Handled
		Failures []*CleanFailure
	}

	// Handled is part of the CleanReport which indicates the executions which were handled
	Handled struct {
		ExecutionCount int64
		CleanedCount int64
		SkippedCount int64
		FailedCount int64
	}

	// CleanFailure indicates a failure to clean
	CleanFailure struct {
		Note string
		Details string
	}
)


package common

import (
	"errors"
	"github.com/uber/cadence/common/persistence"
)

type (
	// CheckResultType is the result type of running a check
	CheckResultType string
	// CleanResultType is the result type of running a clean
	CleanResultType string
)

const (
	// CheckResultTypeFailed indicates a failure occurred while attempting to run check
	CheckResultTypeFailed CheckResultType = "failed"
	// CheckResultTypeCorrupted indicates check successfully ran and detected a corruption
	CheckResultTypeCorrupted = "corrupted"
	// CheckResultTypeHealthy indicates check successfully ran and detected no corruption
	CheckResultTypeHealthy = "healthy"

	// CleanResultTypeSkipped indicates that clean skipped execution
	CleanResultTypeSkipped CleanResultType = "skipped"
	// CleanResultTypeCleaned indicates that clean successfully cleaned an execution
	CleanResultTypeCleaned = "cleaned"
	// CleanResultTypeFailed indciates that clean attempted to clean an execution but failed to do so
	CleanResultTypeFailed = "failed"
)

var (
	// ErrIteratorEmpty indicates that iterator is empty
	ErrIteratorEmpty = errors.New("iterator is empty")
)

// the following are types related to checker
type (
	// CheckRequest represents an execution that should be checked
	CheckRequest struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		TreeID     string
		BranchID   string
		State      int
	}

	// CheckResources contains a union of resources which is populated by checks.
	// Checks which are depend on other checks can access resources using this.
	CheckResources struct {
		History *persistence.InternalReadHistoryBranchResponse
	}

	// CheckResult indicates the result of running a check.
	CheckResult struct {
		CheckResultType  CheckResultType
		Note string
		Details string
	}

	// CheckRequestIteratorResult wraps a CheckRequest and an error.
	// If Error is nil it is guaranteed that CheckRequest is valid.
	CheckRequestIteratorResult struct {
		CheckRequest CheckRequest
		Error        error
	}
)

// the following are serializable types that represent the reports returns by scan and clean
type (
	// ShardScanReport is the report of running scan on a single shard
	ShardScanReport struct {
		ShardID int
		ScanHandled ShardScanHandled
		ControlFlowFailures []ControlFlowFailure
	}

	// ShardScanHandled indicates the executions which were handled by shard scan
	ShardScanHandled struct {
		ExecutionsCount int64
		CorruptedCount  int64
		CheckFailedCount     int64
		CorruptionByType map[string]int64
		CorruptOpenCount int64
	}

	// ShardCleanReport is the report of running clean on a single shard
	ShardCleanReport struct {
		ShardID int
		CleanHandled ShardCleanHandled
		ControlFlowFailures []ControlFlowFailure
	}

	// ShardCleanHandled indicates the executions which were successfully cleaned by clean
	ShardCleanHandled struct {
		ExecutionCount int64
		CleanedCount int64
		SkippedCount int64
		FailedCount int64
	}

	// ControlFlowFailures indicate a failure in scan or clean that is outside of the context
	// of failing to simply scan or clean a single execution. They relate to the control flow of the scan or clean.
	ControlFlowFailure struct {
		Note string
		Details string
	}
)

// the following are serializable types which get output by scan and clean to durable sinks
type (
	// ScanOutputEntity represents a single execution that should be durably recorded by scan.
	ScanOutputEntity struct {
		CheckRequest CheckRequest
		CheckResponse CheckResult
	}

	// CleanOutputEntity represents a single execution that should be durably recorded by clean.
	CleanOutputEntity struct {
		ScanOutputEntity ScanOutputEntity
		CleanAttemptInfo CleanAttemptInfo
	}

	// CleanAttemptInfo represents information about the clean attempt of a single execution
	CleanAttemptInfo struct {
		ResultType CleanResultType
		Note string
		Details string
	}
)
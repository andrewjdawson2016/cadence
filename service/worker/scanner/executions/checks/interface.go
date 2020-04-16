package checks

import "github.com/uber/cadence/common/persistence"

type (
	// CheckType indicates the type of check
	CheckType string
	// CheckResult indicates the result of running a check
	ResultType string
)

const (
	// ResultTypeFailed indicates check could not be ran
	ResultTypeFailed ResultType = "failed"
	// ResultTypeCorrupted indicates the check successfully found corruption
	ResultTypeCorrupted = "corrupted"
	// ResultTypeHealthy indicates the check successfully found no corruption
	ResultTypeHealthy = "healthy"
)

type (
	Checker interface {
		// Check is used to check that an invariant holds for a single execution.
		Check(*CheckRequest) *CheckResponse
		// CheckType returns the type of check
		CheckType() CheckType
	}

	// CheckResponse is the response from Check.
	// Exactly one of FailedResult, CorruptedResult or HealthyResult will be non-nil
	CheckResponse struct {
		ResultType ResultType
		FailedResult *FailedResult
		CorruptedResult *CorruptedResult
		HealthyResult *HealthyResult
	}

	// FailedResult contains details for ResultType=ResultTypeFailed
	FailedResult struct {
		Note string
		Details string
	}

	// CorruptedResult contains details for ResultType=ResultTypeCorrupted
	CorruptedResult struct {
		Note string
		Details string
	}

	// HealthyResult contains details for ResultType=ResultTypeHealthy
	HealthyResult struct {
		Note string
	}

	// CheckRequest is the request to Check
	CheckRequest struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		TreeID     string
		BranchID   string
		State      int
		Resources RequestResources
	}

	// RequestResources enables a check to provide resources which will be used by dependent checks
	RequestResources struct {
		History *persistence.InternalReadHistoryBranchResponse
		// add other resources to future dependent checks here
	}
)


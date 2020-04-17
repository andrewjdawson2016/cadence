package shard

import (
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
)

type (
	CleanResultType string

	ScannedRecordedEntity struct {
		CheckRequest checks.CheckRequest
		CheckResponse checks.CheckResponse
	}

	CleanedRecordedEntity struct {
		ScannedRecordedEntity ScannedRecordedEntity
		CleanAttemptInfo CleanAttemptInfo
	}

	CleanAttemptInfo struct {
		ResultType CleanResultType
		Note string
		Details string
	}
)

const (
	CleanResultTypeSkipped CleanResultType = "skipped"
	CleanResultTypeCleaned = "cleaned"
	CleanResultTypeFailed = "failed"
)
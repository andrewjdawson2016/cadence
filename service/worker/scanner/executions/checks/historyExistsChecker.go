package checks

import (
	"github.com/gocql/gocql"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

const historyPageSize = 1

type (
	historyExistsChecker struct {
		persistenceRetryer common.PersistenceRetryer
	}
)

// NewHistoryExistsChecker constructs a historyExistsChecker
func NewHistoryExistsChecker(persistenceRetryer common.PersistenceRetryer) Checker {
	return &historyExistsChecker{
		persistenceRetryer: persistenceRetryer,
	}
}

// Check checks that history exists
func (c *historyExistsChecker) Check(cr CheckRequest, resources *CheckResources) CheckResponse {
	if !validRequest(cr) {
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note: "invalid request",
			},
		}
	}
	readHistoryBranchReq := &persistence.InternalReadHistoryBranchRequest{
		TreeID:    cr.TreeID,
		BranchID:  cr.BranchID,
		MinNodeID: common.FirstEventID,
		MaxNodeID: common.EndEventID,
		ShardID:   cr.ShardID,
		PageSize:  historyPageSize,
	}
	history, historyErr := c.persistenceRetryer.ReadHistoryBranch(readHistoryBranchReq)
	stillExists, executionErr := concreteExecutionStillExists(cr, c.persistenceRetryer)
	if executionErr != nil {
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note:    "failed to verify if concrete execution still exists",
				Details: executionErr.Error(),
			},
		}
	}
	if !stillExists {
		return CheckResponse{
			ResultType: ResultTypeHealthy,
			HealthyResult: &HealthyResult{
				Note: "concrete execution no longer exists",
			},
		}
	}
	if historyErr != nil {
		if historyErr == gocql.ErrNotFound {
			return CheckResponse{
				ResultType: ResultTypeCorrupted,
				CorruptedResult: &CorruptedResult{
					Note:    "concrete execution exists but history does not",
					Details: historyErr.Error(),
				},
			}
		}
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note:    "failed to verify if history exists",
				Details: historyErr.Error(),
			},
		}
	}
	if history == nil || len(history.History) == 0 {
		return CheckResponse{
			ResultType: ResultTypeCorrupted,
			CorruptedResult: &CorruptedResult{
				Note: "concrete execution exists but got empty history",
			},
		}
	}
	resources.History = history
	return CheckResponse{
		ResultType: ResultTypeHealthy,
		HealthyResult: &HealthyResult{
			Note: "concrete execution exists and history exists",
		},
	}
}

func (c *historyExistsChecker) CheckType() string {
	return "history_exists"
}

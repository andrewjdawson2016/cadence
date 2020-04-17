package checks

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

type (
	orphanExecutionChecker struct {
		persistenceRetryer util.PersistenceRetryer
	}
)

// NewOrphanChecker constructs an orphanExecutionChecker
func NewOrphanChecker(persistenceRetryer util.PersistenceRetryer) Checker {
	return &orphanExecutionChecker{
		persistenceRetryer: persistenceRetryer,
	}
}

func (c *orphanExecutionChecker) Check(cr CheckRequest, _ *CheckResources) CheckResponse {
	if !validRequest(cr) {
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note: "invalid request",
			},
		}
	}
	if !ExecutionOpen(cr) {
		return CheckResponse{
			ResultType: ResultTypeHealthy,
			HealthyResult: &HealthyResult{
				Note: "execution is determined to be healthy because concrete execution is not open or doesn't exist",
			},
		}
	}
	getCurrentExecutionRequest := &persistence.GetCurrentExecutionRequest{
		DomainID:   cr.DomainID,
		WorkflowID: cr.WorkflowID,
	}
	currentExecution, currentErr := c.persistenceRetryer.GetCurrentExecution(getCurrentExecutionRequest)
	stillOpen, concreteErr := concreteExecutionStillOpen(cr, c.persistenceRetryer)
	if concreteErr != nil {
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note:    "failed to check if concrete execution is still open",
				Details: concreteErr.Error(),
			},
		}
	}
	if !stillOpen {
		return CheckResponse{
			ResultType: ResultTypeHealthy,
			HealthyResult: &HealthyResult{
				Note: "execution is determined to be healthy because concrete execution is not open or doesn't exist",
			},
		}
	}
	if currentErr != nil {
		switch currentErr.(type) {
		case *shared.EntityNotExistsError:
			return CheckResponse{
				ResultType: ResultTypeCorrupted,
				CorruptedResult: &CorruptedResult{
					Note:    "execution is open without having current execution",
					Details: currentErr.Error(),
				},
			}
		}
		return CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note:    "failed to check if current execution exists",
				Details: currentErr.Error(),
			},
		}
	}
	if currentExecution.RunID != cr.RunID {
		return CheckResponse{
			ResultType: ResultTypeCorrupted,
			CorruptedResult: &CorruptedResult{
				Note: "execution is open but current points at a different execution",
			},
		}
	}
	return CheckResponse{
		ResultType: ResultTypeHealthy,
		HealthyResult: &HealthyResult{
			Note: "concrete and current both exist, concrete is open and current points at it",
		},
	}
}

func (c *orphanExecutionChecker) CheckType() string {
	return "orphan"
}

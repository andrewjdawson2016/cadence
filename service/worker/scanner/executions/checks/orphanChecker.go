package checks

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

type (
	orphanExecutionChecker struct {
		dbRateLimiter     *quotas.DynamicRateLimiter
		executionStore    persistence.ExecutionStore
		payloadSerializer persistence.PayloadSerializer
	}
)

func newOrphanExecutionChecker(
	dbRateLimiter *quotas.DynamicRateLimiter,
	executionStore persistence.ExecutionStore,
	payloadSerializer persistence.PayloadSerializer,
) checker {
	return &orphanExecutionChecker{
		dbRateLimiter:     dbRateLimiter,
		executionStore:    executionStore,
		payloadSerializer: payloadSerializer,
	}
}

func (c *orphanExecutionChecker) check(cr *checkRequest) *checkResponse {
	result := &checkResponse{
		checkType: checkTypeOrphanExecution,
	}
	if !executionOpen(cr) {
		result.checkResponseStatus = checkResponseHealthy
		return result
	}
	getCurrentExecutionRequest := &persistence.GetCurrentExecutionRequest{
		DomainID:   cr.domainID,
		WorkflowID: cr.workflowID,
	}
	currentExecution, currentErr := persistence.RetryGetCurrentExecution(
		c.dbRateLimiter,
		&result.totalDatabaseRequests,
		c.executionStore,
		getCurrentExecutionRequest)

	stillOpen, concreteErr := concreteExecutionStillOpen(cr, c.executionStore, c.dbRateLimiter, &result.totalDatabaseRequests)
	if concreteErr != nil {
		result.checkResponseStatus = checkResponseFailed
		result.errorInfo = &errorInfo{
			note:    "failed to check if concrete execution is still open",
			details: concreteErr.Error(),
		}
		return result
	}
	if !stillOpen {
		result.checkResponseStatus = checkResponseHealthy
		return result
	}

	if currentErr != nil {
		switch currentErr.(type) {
		case *shared.EntityNotExistsError:
			result.checkResponseStatus = checkResponseCorrupted
			result.errorInfo = &errorInfo{
				note:    "execution is open without having current execution",
				details: currentErr.Error(),
			}
			return result
		}
		result.checkResponseStatus = checkResponseFailed
		result.errorInfo = &errorInfo{
			note:    "failed to check if current execution exists",
			details: currentErr.Error(),
		}
		return result
	}
	if currentExecution.RunID != cr.runID {
		result.checkResponseStatus = checkResponseCorrupted
		result.errorInfo = &errorInfo{
			note: "execution is open but current points at a different execution",
		}
		return result
	}
	result.checkResponseStatus = checkResponseHealthy
	return result
}

func (c *orphanExecutionChecker) validRequest(cr *checkRequest) bool {
	return validRequestHelper(cr)
}

package common

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

// ExecutionOpen returns true if CheckRequest is for an open execution false otherwise
func ExecutionOpen(cr CheckRequest) bool {
	return cr.State == persistence.WorkflowStateCreated || cr.State == persistence.WorkflowStateRunning
}

// StillOpen is used to determine if a workflow is still open.
func StillOpen(cr CheckRequest, persistenceRetryer PersistenceRetryer) (bool, error) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		DomainID: cr.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: &cr.WorkflowID,
			RunId:      &cr.RunID,
		},
	}
	_, err := persistenceRetryer.GetWorkflowExecution(getConcreteExecution)

	if err != nil {
		switch err.(type) {
		case *shared.EntityNotExistsError:
			return false, nil
		default:
			return false, err
		}
	}
	return ExecutionOpen(cr), nil
}

// StillExists is used to determine if a workflow still exists
func StillExists(cr CheckRequest, persistenceRetryer PersistenceRetryer) (bool, error) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		DomainID: cr.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: &cr.WorkflowID,
			RunId:      &cr.RunID,
		},
	}
	_, err := persistenceRetryer.GetWorkflowExecution(getConcreteExecution)
	if err == nil {
		return true, nil
	}

	switch err.(type) {
	case *shared.EntityNotExistsError:
		return false, nil
	default:
		return false, err
	}
}

// ValidRequest returns true if CheckRequest is valid, false otherwise.
// TODO: consume this method
func ValidCheckRequest(cr CheckRequest) bool {
	return len(cr.DomainID) > 0 &&
		len(cr.WorkflowID) > 0 &&
		len(cr.RunID) > 0 &&
		len(cr.TreeID) > 0 &&
		len(cr.BranchID) > 0
}

// ValidCheckRequestIteratorResult returns true if CheckRequestIteratorResult is valid, false otherwise.
func ValidCheckRequestIteratorResult(itrResult *CheckRequestIteratorResult) bool {
	if itrResult.Error == nil {
		return ValidCheckRequest(itrResult.CheckRequest)
	}
	return true
}
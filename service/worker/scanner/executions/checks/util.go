package checks

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

func concreteExecutionStillOpen(cr *CheckRequest, persistenceRetryer util.PersistenceRetryer) (bool, error) {
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
	return executionOpen(cr), nil
}

func concreteExecutionStillExists(cr *CheckRequest, persistenceRetryer util.PersistenceRetryer) (bool, error) {
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

func executionOpen(cr *CheckRequest) bool {
	return cr.State == persistence.WorkflowStateCreated ||
		cr.State == persistence.WorkflowStateRunning
}

func validRequest(cr *CheckRequest) bool {
	return len(cr.DomainID) > 0 &&
		len(cr.WorkflowID) > 0 &&
		len(cr.RunID) > 0 &&
		len(cr.TreeID) > 0 &&
		len(cr.BranchID) > 0
}

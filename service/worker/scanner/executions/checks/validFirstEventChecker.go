package checks

import (
	"fmt"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

type (
	validFirstEventChecker struct {
		payloadSerializer persistence.PayloadSerializer
	}
)

func newValidFirstEventChecker(
	payloadSerializer persistence.PayloadSerializer,
) checker {
	return &validFirstEventChecker{
		payloadSerializer: payloadSerializer,
	}
}

func (c *validFirstEventChecker) check(cr *checkRequest) *checkResponse {
	result := &checkResponse{
		checkType: checkTypeValidFirstEvent,
	}
	history := cr.prerequisiteCheckPayload.(*persistence.InternalReadHistoryBranchResponse)
	firstBatch, err := c.payloadSerializer.DeserializeBatchEvents(history.History[0])
	if err != nil || len(firstBatch) == 0 {
		result.checkResponseStatus = checkResponseFailed
		result.errorInfo = &errorInfo{
			note:    "failed to deserialize batch events",
			details: err.Error(),
		}
		return result
	}
	if firstBatch[0].GetEventId() != common.FirstEventID {
		result.checkResponseStatus = checkResponseCorrupted
		result.errorInfo = &errorInfo{
			note:    "got unexpected first eventID",
			details: fmt.Sprintf("expected: %v but got %v", common.FirstEventID, firstBatch[0].GetEventId()),
		}
		return result
	}
	if firstBatch[0].GetEventType() != shared.EventTypeWorkflowExecutionStarted {
		result.checkResponseStatus = checkResponseCorrupted
		result.errorInfo = &errorInfo{
			note:    "got unexpected first eventType",
			details: fmt.Sprintf("expected: %v but got %v", shared.EventTypeWorkflowExecutionStarted.String(), firstBatch[0].GetEventType().String()),
		}
		return result
	}
	result.checkResponseStatus = checkResponseHealthy
	return result
}

func (c *validFirstEventChecker) validRequest(cr *checkRequest) bool {
	if cr.prerequisiteCheckPayload == nil {
		return false
	}
	history, ok := cr.prerequisiteCheckPayload.(*persistence.InternalReadHistoryBranchResponse)
	if !ok {
		return false
	}
	if history.History == nil || len(history.History) == 0 {
		return false
	}
	return validRequestHelper(cr)
}
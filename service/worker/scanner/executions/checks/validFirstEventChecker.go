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

// NewValidFirstEventChecker constructs a validFirstEventChecker
func NewValidFirstEventChecker(
	payloadSerializer persistence.PayloadSerializer,
) Checker {
	return &validFirstEventChecker{
		payloadSerializer: payloadSerializer,
	}
}

func (c *validFirstEventChecker) Check(cr *CheckRequest) *CheckResponse {
	if !validRequest(cr) {
		return &CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note: "invalid request",
			},
		}
	}
	if cr.Resources.History == nil || len(cr.Resources.History.History) == 0 {
		return &CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note: "invalid request, resources.history is not set",
			},
		}
	}
	firstBatch, err := c.payloadSerializer.DeserializeBatchEvents(cr.Resources.History.History[0])
	if err != nil || len(firstBatch) == 0 {
		details := ""
		if err != nil {
			details = err.Error()
		}
		return &CheckResponse{
			ResultType: ResultTypeFailed,
			FailedResult: &FailedResult{
				Note:    "failed to deserialize batch events",
				Details: details,
			},
		}
	}
	firstEvent := firstBatch[0]
	if firstEvent.GetEventId() != common.FirstEventID {
		return &CheckResponse{
			ResultType: ResultTypeCorrupted,
			CorruptedResult: &CorruptedResult{
				Note:    "got unexpected first eventID",
				Details: fmt.Sprintf("expected: %v but got %v", common.FirstEventID, firstEvent.GetEventId()),
			},
		}
	}
	if firstEvent.GetEventType() != shared.EventTypeWorkflowExecutionStarted {
		return &CheckResponse{
			ResultType: ResultTypeCorrupted,
			CorruptedResult: &CorruptedResult{
				Note:    "got unexpected first eventType",
				Details: fmt.Sprintf("expected: %v but got %v", shared.EventTypeWorkflowExecutionStarted.String(), firstEvent.GetEventType().String()),
			},
		}
	}
	return &CheckResponse{
		ResultType: ResultTypeHealthy,
		HealthyResult: &HealthyResult{
			Note: "got valid first history event",
		},
	}
}

func (c *validFirstEventChecker) CheckType() CheckType {
	return "valid_first_event"
}

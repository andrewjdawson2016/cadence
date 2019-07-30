package history

import (
	"errors"
	"fmt"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	// QueryState indicates the state a query is in
	QueryState int

	// Query defines a user input query, a result and a state of handling that query. Query is not thread safe.
	Query interface {
		Id() string
		WorkflowQuery() *shared.WorkflowQuery
		WorkflowQueryResult() *shared.WorkflowQueryResult
		State() QueryState
		DeepCopy() Query
		StateChangeCh() <-chan QueryState

		DependentEventsPersisted()
		SetResult(*shared.WorkflowQueryResult) error
		Buffer() error
		Start() error
		Expire() error
	}

	query struct {
		id string
		workflowQuery *shared.WorkflowQuery
		workflowQueryResult *shared.WorkflowQueryResult
		dependentEventsPersisted bool
		state QueryState
	}
)

const (
	// QueryStateNone is the starting state for all queries
	QueryStateNone QueryState = iota
	// QueryStateBuffered indicates that query is awaiting dispatch
	QueryStateBuffered
	// QueryStateStarted indicates the decision task containing the query has been started
	QueryStateStarted
	// QueryStateCompleted indicates that a query result has been received and any dependent events have been recorded in history
	QueryStateCompleted
	// QueryStateExpired means the query timed out before it was completed
	QueryStateExpired
)

// String returns a string representation of QueryState
func (qs QueryState) String() string {
	switch qs {
	case QueryStateNone:
		return "none"
	case QueryStateBuffered:
		return "buffered"
	case QueryStateStarted:
		return "started"
	case QueryStateCompleted:
		return "completed"
	case QueryStateExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// Terminal indicates if a QueryState is a terminal state
func (qs QueryState) Terminal() bool {
	return qs == QueryStateExpired || qs == QueryStateCompleted
}

// NewQuery constructs a new Query
func NewQuery(workflowQuery *shared.WorkflowQuery) Query {
	return &query{
		id: uuid.New(),
		workflowQuery: copyWorkflowQuery(workflowQuery),
		workflowQueryResult: nil,
		state: QueryStateBuffered,
	}
}

// Id returns a unique identifier for this query
func (q *query) Id() string {
	return q.id
}

// WorkflowQuery returns the user's workflowQuery
func (q *query) WorkflowQuery() *shared.WorkflowQuery {
	return copyWorkflowQuery(q.workflowQuery)
}

// WorkflowQueryResult returns the user's workflowQueryResult
func (q *query) WorkflowQueryResult() *shared.WorkflowQueryResult {
	return copyWorkflowQueryResult(q.workflowQueryResult)
}

// State returns the state of the query
func (q *query) State() QueryState {
	return q.state
}

// DeepCopy returns a deep copy of this query
func (q *query) DeepCopy() Query {
	return &query{
		id:                  q.id,
		workflowQuery:       copyWorkflowQuery(q.workflowQuery),
		workflowQueryResult: copyWorkflowQueryResult(q.workflowQueryResult),
		state:               q.state,
	}
}

// DependentEventsPersisted indicates that the events this query was reliant on have been persisted
func (q *query) DependentEventsPersisted() {
	q.dependentEventsPersisted = true
	if q.shouldTransitionToCompleted() {
		q.state = QueryStateCompleted
	}
}

// SetResult sets the workflowQueryResult, returns an error if result has already been set.
func (q *query) SetResult(workflowQueryResult *shared.WorkflowQueryResult) error {
	if q.workflowQueryResult != nil {
		return errors.New("workflow query result is already set cannot update")
	}
	q.workflowQueryResult = copyWorkflowQueryResult(workflowQueryResult)
	if q.shouldTransitionToCompleted() {
		q.state = QueryStateCompleted
	}
	return nil
}

// Buffer sets the query state to buffered
func (q *query) Buffer() error {
	if q.state == QueryStateNone || q.state == QueryStateStarted {
		q.state = QueryStateBuffered
		return nil
	}
	return fmt.Errorf("cannot transition from %v to %v", q.state, QueryStateBuffered)
}

// Start sets the query state to started
func (q *query) Start() error {
	if q.state == QueryStateBuffered {
		q.state = QueryStateStarted
		return nil
	}
	return fmt.Errorf("cannot transition from %v to %v", q.state, QueryStateStarted)
}

// Expire sets the query state to expired
func (q *query) Expire() error {
	if q.state.Terminal() {
		return fmt.Errorf("cannot transition out of terminal state %v", q.state)
	}
	q.state = QueryStateExpired
	return nil
}

func (q *query) shouldTransitionToCompleted() bool {
	return q.state == QueryStateStarted && q.workflowQueryResult != nil &&  q.dependentEventsPersisted
}

func copyWorkflowQuery(input *shared.WorkflowQuery) *shared.WorkflowQuery {
	if input == nil {
		return nil
	}
	return &shared.WorkflowQuery{
		QueryType: common.StringPtr(input.GetQueryType()),
		QueryArgs: copyByteSlice(input.GetQueryArgs()),
	}
}

func copyWorkflowQueryResult(input *shared.WorkflowQueryResult) *shared.WorkflowQueryResult {
	if input == nil {
		return nil
	}
	return &shared.WorkflowQueryResult{
		ResultType: common.QueryResultTypePtr(input.GetResultType()),
		Answer: copyByteSlice(input.GetAnswer()),
		ErrorReason: common.StringPtr(input.GetErrorReason()),
		ErrorDetails: copyByteSlice(input.GetErrorDetails()),
	}
}

func copyByteSlice(input []byte) []byte {
	result := make([]byte, len(input), len(input))
	copy(result, input)
	return result
}


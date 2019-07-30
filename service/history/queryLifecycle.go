package history

import (
	"errors"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"sync"
	"time"
)

type (
	CompleteConditionFn func(*WatcherSnapshot) bool

	QueryLifecycle interface {
		GetQuery() Query
		RebufferQuery() error
		StartQuery(CompleteConditionFn) error
		RecordQueryResult(*shared.WorkflowQueryResult) error
	}

	queryLifecycle struct {
		sync.Mutex

		logger log.Logger
		ttl time.Duration
		watcher WorkflowWatcher
		query Query

		// add other things needed to handle updating a query
	}
)

func NewQueryLifecycle(
	logger log.Logger,
	ttl time.Duration,
	watcher WorkflowWatcher,
	workflowQuery *shared.WorkflowQuery,


) QueryLifecycle {
	queryLifecycle := &queryLifecycle{
		logger: logger,
		ttl: ttl,
		watcher: watcher,
		query: NewQuery(workflowQuery),
	}
	go queryLifecycle.runExpireChecker()
	return queryLifecycle
}

func (l *queryLifecycle) GetQuery() Query {
	l.Lock()
	defer l.Unlock()
	return l.query.DeepCopy()
}

// RebufferQuery is used to rebuffer a failed query after it has already been started
func (l *queryLifecycle) RebufferQuery() error {
	l.Lock()
	defer l.Unlock()
	if l.query.State() != QueryStateStarted {
		return errors.New("queries can only re-enter the buffered state from the started state")
	}
	return l.query.Buffer()
}

func (l *queryLifecycle) StartQuery(fn CompleteConditionFn) error {
	l.Lock()
	defer l.Unlock()
	if err := l.query.Start(); err != nil {
		return err
	}
	// start a go routine to handle the complete of condition function
	return nil
}

func (l *queryLifecycle) RecordQueryResult(result *shared.WorkflowQueryResult) error {
	l.Lock()
	defer l.Unlock()
	return l.query.SetResult(result)
}

func (l *queryLifecycle) runExpireChecker() {
	<-time.After(l.ttl)

	l.Lock()
	defer l.Unlock()
	if err := l.query.SetState(QueryStateExpired); err != nil {
		l.logger.Error("failed to expire query", tag.Error(err))
	}
}

func (l *queryLifecycle) runCompleteChecker() {
	select {

	}

	// after workflow watcher indicates that events have been persisted
	// update PersistedEventsRecorded to be true
	//
}
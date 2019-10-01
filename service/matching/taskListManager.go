// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/.gen/go/matching"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// pollForActivityTask or pollForDecisionTask handler.
	returnEmptyTaskTimeBudget time.Duration = time.Second
)

type (
	addTaskParams struct {
		execution     *s.WorkflowExecution
		taskInfo      *persistence.TaskInfo
		forwardedFrom string
	}

	addInMemoryParams struct {
		domainID               string
		execution *s.WorkflowExecution
		forwardedFrom string
	}

	taskListManager interface {
		Start() error
		Stop()
		// AddTask adds a task to the task list. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// AddInMemoryTask adds a task to the task list. This method will only attempt a synchronous match with a poller.
		AddInMemoryTask(ctx context.Context, params addInMemoryParams) error
		// GetTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task list to pollers
		GetTask(ctx context.Context, maxDispatchPerSecond *float64) (*internalTask, error)
		// DispatchTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchTask(ctx context.Context, task *internalTask) error
		// DispatchQueryTask dispatches a query task to a poller. When there are no pollers
		// to pick up the task, this method will return error. Task will not be persisted to
		// db and no ratelimits are applied for this call
		DispatchQueryTask(ctx context.Context, taskID string, request *matching.QueryWorkflowRequest) ([]byte, error)
		CancelPoller(pollerID string)
		GetAllPollerInfo() []*s.PollerInfo
		// DescribeTaskList returns information about the target tasklist
		DescribeTaskList(includeTaskListStatus bool) *s.DescribeTaskListResponse
		String() string
	}

	// Single task list in memory state
	taskListManagerImpl struct {
		taskListID       *taskListID
		taskListKind     int // sticky taskList has different process in persistence
		config           *taskListConfig
		db               *taskListDB
		engine           *matchingEngineImpl
		taskWriter       *taskWriter
		taskReader       *taskReader // reads tasks from db and async matches it with poller
		taskGC           *taskGC
		taskAckManager   ackManager   // tracks ackLevel for delivered messages
		matcher          *TaskMatcher // for matching a task producer with a poller
		domainCache      cache.DomainCache
		logger           log.Logger
		metricsClient    metrics.Client
		domainNameValue  atomic.Value
		domainScopeValue atomic.Value // domain tagged metric scope
		// pollerHistory stores poller which poll from this tasklist in last few minutes
		pollerHistory *pollerHistory
		// outstandingPollsMap is needed to keep track of all outstanding pollers for a
		// particular tasklist.  PollerID generated by frontend is used as the key and
		// CancelFunc is the value.  This is used to cancel the context to unblock any
		// outstanding poller when the frontend detects client connection is closed to
		// prevent tasks being dispatched to zombie pollers.
		outstandingPollsLock sync.Mutex
		outstandingPollsMap  map[string]context.CancelFunc

		shutdownCh chan struct{}  // Delivers stop to the pump that populates taskBuffer
		startWG    sync.WaitGroup // ensures that background processes do not start until setup is ready
		stopped    int32
	}
)

const (
	// maxSyncMatchWaitTime is the max amount of time that we are willing to wait for a sync match to happen
	maxSyncMatchWaitTime = 200 * time.Millisecond
)

var _ taskListManager = (*taskListManagerImpl)(nil)

func newTaskListManager(
	e *matchingEngineImpl,
	taskList *taskListID,
	taskListKind *s.TaskListKind,
	config *Config,
) (taskListManager, error) {

	taskListConfig, err := newTaskListConfig(taskList, config, e.domainCache)
	if err != nil {
		return nil, err
	}

	if taskListKind == nil {
		taskListKind = common.TaskListKindPtr(s.TaskListKindNormal)
	}

	db := newTaskListDB(e.taskManager, taskList.domainID, taskList.name, taskList.taskType, int(*taskListKind), e.logger)
	tlMgr := &taskListManagerImpl{
		domainCache:   e.domainCache,
		metricsClient: e.metricsClient,
		engine:        e,
		shutdownCh:    make(chan struct{}),
		taskListID:    taskList,
		logger: e.logger.WithTags(tag.WorkflowTaskListName(taskList.name),
			tag.WorkflowTaskListType(taskList.taskType)),
		db:                  db,
		taskAckManager:      newAckManager(e.logger),
		taskGC:              newTaskGC(db, taskListConfig),
		config:              taskListConfig,
		pollerHistory:       newPollerHistory(),
		outstandingPollsMap: make(map[string]context.CancelFunc),
		taskListKind:        int(*taskListKind),
	}
	tlMgr.domainNameValue.Store("")
	tlMgr.domainScopeValue.Store(e.metricsClient.Scope(metrics.MatchingTaskListMgrScope, metrics.DomainUnknownTag()))
	tlMgr.tryInitDomainNameAndScope()
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)
	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskList, *taskListKind) {
		fwdr = newForwarder(&taskListConfig.forwarderConfig, taskList, *taskListKind, e.matchingClient, tlMgr.domainScope)
	}
	tlMgr.matcher = newTaskMatcher(taskListConfig, fwdr, tlMgr.domainScope)
	tlMgr.startWG.Add(1)
	return tlMgr, nil
}

// Starts reading pump for the given task list.
// The pump fills up taskBuffer from persistence.
func (c *taskListManagerImpl) Start() error {
	defer c.startWG.Done()

	// Make sure to grab the range first before starting task writer, as it needs the range to initialize maxReadLevel
	state, err := c.renewLeaseWithRetry()
	if err != nil {
		c.Stop()
		return err
	}

	c.taskAckManager.setAckLevel(state.ackLevel)
	c.taskWriter.Start(c.rangeIDToTaskIDBlock(state.rangeID))
	c.taskReader.Start()

	return nil
}

// Stops pump that fills up taskBuffer from persistence.
func (c *taskListManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		return
	}
	close(c.shutdownCh)
	c.taskWriter.Stop()
	c.taskReader.Stop()
	c.engine.removeTaskListManager(c.taskListID)
	c.engine.removeTaskListManager(c.taskListID)
	c.logger.Info("", tag.LifeCycleStopped)
}

// AddTask adds a task to the task list. This method will first attempt a synchronous
// match with a poller. When there are no pollers or if ratelimit is exceeded, task will
// be written to database and later asynchronously matched with a poller
func (c *taskListManagerImpl) AddTask(ctx context.Context, params addTaskParams) (bool, error) {
	c.startWG.Wait()
	var syncMatch bool
	_, err := c.executeWithRetry(func() (interface{}, error) {

		domainEntry, err := c.domainCache.GetDomainByID(params.taskInfo.DomainID)
		if err != nil {
			return nil, err
		}

		if domainEntry.GetDomainNotActiveErr() != nil {
			r, err := c.taskWriter.appendTask(params.execution, params.taskInfo)
			syncMatch = false
			return r, err
		}

		syncMatch, err = c.trySyncMatch(ctx, newInternalTask(params.taskInfo, c.completeTask, params.forwardedFrom, true))
		if syncMatch {
			return &persistence.CreateTasksResponse{}, err
		}

		r, err := c.taskWriter.appendTask(params.execution, params.taskInfo)
		syncMatch = false
		return r, err
	})
	if err == nil {
		c.taskReader.Signal()
	}
	return syncMatch, err
}

// AddInMemoryTask adds a task to the task list. This method will only attempt a synchronous match with a poller.
func (c *taskListManagerImpl) AddInMemoryTask(ctx context.Context, params addInMemoryParams) error {
	c.startWG.Wait()
	_, err := c.executeWithRetry(func() (interface{}, error) {
		syncMatch, err := c.trySyncMatch(ctx, newInternalInMemoryTask(params.forwardedFrom))
		if err != nil {
			return nil, err
		}
		if !syncMatch {
			return nil, errors.New("could not sync match")
		}
		return nil, nil
	})
	return err
}

// DispatchTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *taskListManagerImpl) DispatchTask(ctx context.Context, task *internalTask) error {
	return c.matcher.MustOffer(ctx, task)
}

// DispatchQueryTask dispatches a query task to a poller. When there are no pollers
// to pick up the task, this method will return error. Task will not be persisted to
// db and no ratelimits will be applied for this call
func (c *taskListManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matching.QueryWorkflowRequest,
) ([]byte, error) {
	c.startWG.Wait()
	task := newInternalQueryTask(taskID, request)
	resp, err := c.matcher.OfferQuery(ctx, task)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, &s.QueryFailedError{Message: "timeout: no workflow worker polling for given tasklist"}
		}
		if _, ok := err.(*s.QueryFailedError); ok {
			// this can happen when the query is forwarded to a parent partition
			return nil, err
		}
		return nil, &s.QueryFailedError{Message: err.Error()}
	}
	return resp, nil
}

// GetTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task list to pollers
func (c *taskListManagerImpl) GetTask(
	ctx context.Context,
	maxDispatchPerSecond *float64,
) (*internalTask, error) {
	task, err := c.getTask(ctx, maxDispatchPerSecond)
	if err != nil {
		return nil, err
	}
	task.domainName = c.domainName()
	task.backlogCountHint = c.taskAckManager.getBacklogCountHint()
	return task, nil
}

func (c *taskListManagerImpl) getTask(ctx context.Context, maxDispatchPerSecond *float64) (*internalTask, error) {
	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	childCtx, cancel := c.newChildContext(ctx, c.config.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	pollerID, ok := ctx.Value(pollerIDKey).(string)
	if ok && pollerID != "" {
		// Found pollerID on context, add it to the map to allow it to be canceled in
		// response to CancelPoller call
		c.outstandingPollsLock.Lock()
		c.outstandingPollsMap[pollerID] = cancel
		c.outstandingPollsLock.Unlock()
		defer func() {
			c.outstandingPollsLock.Lock()
			delete(c.outstandingPollsMap, pollerID)
			c.outstandingPollsLock.Unlock()
		}()
	}

	identity, ok := ctx.Value(identityKey).(string)
	if ok && identity != "" {
		c.pollerHistory.updatePollerInfo(pollerIdentity(identity), maxDispatchPerSecond)
	}

	domainEntry, err := c.domainCache.GetDomainByID(c.taskListID.domainID)
	if err != nil {
		return nil, err
	}

	// the desired global rate limit for the task list comes from the
	// poller, which lives inside the client side worker. There is
	// one rateLimiter for this entire task list and as we get polls,
	// we update the ratelimiter rps if it has changed from the last
	// value. Last poller wins if different pollers provide different values
	c.matcher.UpdateRatelimit(maxDispatchPerSecond)

	if domainEntry.GetDomainNotActiveErr() != nil {
		return c.matcher.PollForQuery(childCtx)
	}

	return c.matcher.Poll(childCtx)
}

// GetAllPollerInfo returns all pollers that polled from this tasklist in last few minutes
func (c *taskListManagerImpl) GetAllPollerInfo() []*s.PollerInfo {
	return c.pollerHistory.getAllPollerInfo()
}

func (c *taskListManagerImpl) CancelPoller(pollerID string) {
	c.outstandingPollsLock.Lock()
	cancel, ok := c.outstandingPollsMap[pollerID]
	c.outstandingPollsLock.Unlock()

	if ok && cancel != nil {
		cancel()
	}
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes and status of tasklist's ackManager
// (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (c *taskListManagerImpl) DescribeTaskList(includeTaskListStatus bool) *s.DescribeTaskListResponse {
	response := &s.DescribeTaskListResponse{Pollers: c.GetAllPollerInfo()}
	if !includeTaskListStatus {
		return response
	}

	taskIDBlock := c.rangeIDToTaskIDBlock(c.db.RangeID())
	response.TaskListStatus = &s.TaskListStatus{
		ReadLevel:        common.Int64Ptr(c.taskAckManager.getReadLevel()),
		AckLevel:         common.Int64Ptr(c.taskAckManager.getAckLevel()),
		BacklogCountHint: common.Int64Ptr(c.taskAckManager.getBacklogCountHint()),
		RatePerSecond:    common.Float64Ptr(c.matcher.Rate()),
		TaskIDBlock: &s.TaskIDBlock{
			StartID: common.Int64Ptr(taskIDBlock.start),
			EndID:   common.Int64Ptr(taskIDBlock.end),
		},
	}

	return response
}

func (c *taskListManagerImpl) String() string {
	buf := new(bytes.Buffer)
	if c.taskListID.taskType == persistence.TaskListTypeActivity {
		buf.WriteString("Activity")
	} else {
		buf.WriteString("Decision")
	}
	rangeID := c.db.RangeID()
	fmt.Fprintf(buf, " task list %v\n", c.taskListID.name)
	fmt.Fprintf(buf, "RangeID=%v\n", rangeID)
	fmt.Fprintf(buf, "TaskIDBlock=%+v\n", c.rangeIDToTaskIDBlock(rangeID))
	fmt.Fprintf(buf, "AckLevel=%v\n", c.taskAckManager.ackLevel)
	fmt.Fprintf(buf, "MaxReadLevel=%v\n", c.taskAckManager.getReadLevel())

	return buf.String()
}

// completeTask marks a task as processed. Only tasks created by taskReader (i.e. backlog from db) reach
// here. As part of completion:
//   - task is deleted from the database when err is nil
//   - new task is created and current task is deleted when err is not nil
func (c *taskListManagerImpl) completeTask(task *persistence.TaskInfo, err error) {
	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
		// re-written to persistence frequently.
		_, err = c.executeWithRetry(func() (interface{}, error) {
			wf := &s.WorkflowExecution{WorkflowId: &task.WorkflowID, RunId: &task.RunID}
			return c.taskWriter.appendTask(wf, task)
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task so we just unload the entire task list
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskList,
				tag.Error(err),
				tag.WorkflowTaskListName(c.taskListID.name),
				tag.WorkflowTaskListType(c.taskListID.taskType))
			c.Stop()
			return
		}
		c.taskReader.Signal()
	}
	ackLevel := c.taskAckManager.completeTask(task.TaskID)
	c.taskGC.Run(ackLevel)
}

func (c *taskListManagerImpl) renewLeaseWithRetry() (taskListState, error) {
	var newState taskListState
	op := func() (err error) {
		newState, err = c.db.RenewLease()
		return
	}
	c.domainScope().IncCounter(metrics.LeaseRequestCounter)
	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		c.domainScope().IncCounter(metrics.LeaseFailureCounter)
		c.engine.unloadTaskList(c.taskListID)
		return newState, err
	}
	return newState, nil
}

func (c *taskListManagerImpl) rangeIDToTaskIDBlock(rangeID int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*c.config.RangeSize + 1,
		end:   rangeID * c.config.RangeSize,
	}
}

func (c *taskListManagerImpl) allocTaskIDBlock(prevBlockEnd int64) (taskIDBlock, error) {
	currBlock := c.rangeIDToTaskIDBlock(c.db.RangeID())
	if currBlock.end != prevBlockEnd {
		return taskIDBlock{},
			fmt.Errorf("allocTaskIDBlock: invalid state: prevBlockEnd:%v != currTaskIDBlock:%+v", prevBlockEnd, currBlock)
	}
	state, err := c.renewLeaseWithRetry()
	if err != nil {
		return taskIDBlock{}, err
	}
	return c.rangeIDToTaskIDBlock(state.rangeID), nil
}

func (c *taskListManagerImpl) getAckLevel() (ackLevel int64) {
	return c.taskAckManager.getAckLevel()
}

func (c *taskListManagerImpl) getTaskListKind() int {
	// there is no need to lock here,
	// since c.taskListKind is assigned when taskListManager been created and never changed.
	return c.taskListKind
}

// Retry operation on transient error. On rangeID update by another process calls c.Stop().
func (c *taskListManagerImpl) executeWithRetry(
	operation func() (interface{}, error)) (result interface{}, err error) {

	op := func() error {
		result, err = operation()
		return err
	}

	var retryCount int64
	err = backoff.Retry(op, persistenceOperationRetryPolicy, func(err error) bool {
		c.logger.Debug(fmt.Sprintf("Retry executeWithRetry as task list range has changed. retryCount=%v, errType=%T", retryCount, err))
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})

	if _, ok := err.(*persistence.ConditionFailedError); ok {
		c.domainScope().IncCounter(metrics.ConditionFailedErrorCounter)
		c.logger.Debug(fmt.Sprintf("Stopping task list due to persistence condition failure. Err: %v", err))
		c.Stop()
	}
	return
}

func (c *taskListManagerImpl) trySyncMatch(ctx context.Context, task *internalTask) (bool, error) {
	childCtx, cancel := c.newChildContext(ctx, maxSyncMatchWaitTime, time.Second)
	matched, err := c.matcher.Offer(childCtx, task)
	cancel()
	return matched, err
}

// newChildContext creates a child context with desired timeout.
// if tailroom is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-tailroom, timeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func (c *taskListManagerImpl) newChildContext(
	parent context.Context,
	timeout time.Duration,
	tailroom time.Duration,
) (context.Context, context.CancelFunc) {
	select {
	case <-parent.Done():
		return parent, func() {}
	default:
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, timeout)
	}
	remaining := deadline.Sub(time.Now()) - tailroom
	if remaining < timeout {
		timeout = time.Duration(common.MaxInt64(0, int64(remaining)))
	}
	return context.WithTimeout(parent, timeout)
}

func (c *taskListManagerImpl) isFowardingAllowed(taskList *taskListID, kind s.TaskListKind) bool {
	return !taskList.IsRoot() && kind != s.TaskListKindSticky
}

func createServiceBusyError(msg string) *s.ServiceBusyError {
	return &s.ServiceBusyError{Message: msg}
}

func (c *taskListManagerImpl) domainScope() metrics.Scope {
	scope := c.domainScopeValue.Load().(metrics.Scope)
	if scope != nil {
		return scope
	}
	c.tryInitDomainNameAndScope()
	return c.domainScopeValue.Load().(metrics.Scope)
}

func (c *taskListManagerImpl) domainName() string {
	name := c.domainNameValue.Load().(string)
	if len(name) > 0 {
		return name
	}
	c.tryInitDomainNameAndScope()
	return c.domainNameValue.Load().(string)
}

// reload from domainCache in case it got empty result during construction
func (c *taskListManagerImpl) tryInitDomainNameAndScope() {
	domainName := c.domainNameValue.Load().(string)
	if len(domainName) == 0 {
		domainName, scope := domainNameAndMetricScope(c.domainCache, c.taskListID.domainID, c.metricsClient, metrics.MatchingTaskListMgrScope)
		if len(domainName) > 0 && scope != nil {
			c.domainNameValue.Store(domainName)
			c.domainScopeValue.Store(scope)
		}
	}
}

// if domainCache return error, it will return "" as domainName and a scope without domainName tagged
func domainNameAndMetricScope(cache cache.DomainCache, domainID string, client metrics.Client, scope int) (string, metrics.Scope) {
	entry, err := cache.GetDomainByID(domainID)
	if err != nil {
		return "", nil
	}
	return entry.GetInfo().Name, client.Scope(scope, metrics.DomainTag(entry.GetInfo().Name))
}

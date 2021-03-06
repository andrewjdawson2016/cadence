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

package replicator

import (
	"errors"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	messageMocks "github.com/uber/cadence/common/messaging/mocks"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/xdc"
)

type (
	activityReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        bark.Logger
		metricsClient metrics.Client

		mockMsg           *messageMocks.Message
		mockHistoryClient *mocks.HistoryClient
		mockRereplicator  *xdc.MockHistoryRereplicator
	}

	historyReplicationTaskSuite struct {
		suite.Suite
		config        *Config
		logger        bark.Logger
		metricsClient metrics.Client
		sourceCluster string

		mockMsg           *messageMocks.Message
		mockHistoryClient *mocks.HistoryClient
		mockRereplicator  *xdc.MockHistoryRereplicator
	}
)

func TestActivityReplicationTaskSuite(t *testing.T) {
	s := new(activityReplicationTaskSuite)
	suite.Run(t, s)
}

func TestHistoryReplicationTaskSuite(t *testing.T) {
	s := new(historyReplicationTaskSuite)
	suite.Run(t, s)
}

func (s *activityReplicationTaskSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *activityReplicationTaskSuite) TearDownSuite() {

}

func (s *activityReplicationTaskSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.config = &Config{
		ReplicationTaskMaxRetry: dynamicconfig.GetIntPropertyFn(10),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)

	s.mockMsg = &messageMocks.Message{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
}

func (s *activityReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
}

func (s *historyReplicationTaskSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *historyReplicationTaskSuite) TearDownSuite() {

}

func (s *historyReplicationTaskSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.config = &Config{
		ReplicatorHistoryBufferRetryCount: dynamicconfig.GetIntPropertyFn(2),
		ReplicationTaskMaxRetry:           dynamicconfig.GetIntPropertyFn(10),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)
	s.sourceCluster = cluster.TestAlternativeClusterName

	s.mockMsg = &messageMocks.Message{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
}

func (s *historyReplicationTaskSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
}

func (s *activityReplicationTaskSuite) TestNewActivityReplicationTask() {
	replicationTask := s.getActivityReplicationTask()
	replicationAttr := replicationTask.SyncActicvityTaskAttributes

	task := newActivityReplicationTask(replicationTask, s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	// overwrite the logger for easy comparison
	task.logger = s.logger
	s.Equal(
		&activityReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.SyncActivityTaskScope,
				startTime:    task.startTime,
				partitionID: definition.NewWorkflowIdentifier(
					replicationAttr.GetDomainId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:              replicationAttr.GetScheduledId(),
				attempt:             0,
				kafkaMsg:            s.mockMsg,
				logger:              s.logger,
				config:              s.config,
				historyClient:       s.mockHistoryClient,
				metricsClient:       s.metricsClient,
				historyRereplicator: s.mockRereplicator,
			},
			req: &h.SyncActivityRequest{
				DomainId:          replicationAttr.DomainId,
				WorkflowId:        replicationAttr.WorkflowId,
				RunId:             replicationAttr.RunId,
				Version:           replicationAttr.Version,
				ScheduledId:       replicationAttr.ScheduledId,
				ScheduledTime:     replicationAttr.ScheduledTime,
				StartedId:         replicationAttr.StartedId,
				StartedTime:       replicationAttr.StartedTime,
				LastHeartbeatTime: replicationAttr.LastHeartbeatTime,
				Details:           replicationAttr.Details,
				Attempt:           replicationAttr.Attempt,
			},
		},
		task,
	)
}

func (s *activityReplicationTaskSuite) TestExecute() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.On("SyncActivity", mock.Anything, task.req).Return(randomErr).Once()
	err := task.Execute()
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_NotEnoughAttempt() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_NotRetryErr() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *activityReplicationTaskSuite) TestHandleErr_RetryErr() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(task.partitionID.DomainID),
		WorkflowId:  common.StringPtr(task.partitionID.WorkflowID),
		RunId:       common.StringPtr("other random run ID"),
		NextEventId: common.Int64Ptr(447),
	}

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.partitionID.DomainID, task.partitionID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.partitionID.RunID, task.taskID+1,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.partitionID.DomainID, task.partitionID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.partitionID.RunID, task.taskID+1,
	).Return(nil).Once()
	s.mockHistoryClient.On("SyncActivity", mock.Anything, task.req).Return(nil).Once()
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *activityReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := &shared.BadRequestError{}
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable() {
	err := &shared.InternalServiceError{}
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := &shared.InternalServiceError{}
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetry() + 100
	s.False(task.RetryErr(err))
}

func (s *activityReplicationTaskSuite) TestAck() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *activityReplicationTaskSuite) TestNack() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *activityReplicationTaskSuite) TestQueueID() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.Equal(task.partitionID, task.PartitionID())
}

func (s *activityReplicationTaskSuite) TestTaskID() {
	task := newActivityReplicationTask(s.getActivityReplicationTask(), s.mockMsg, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.Equal(task.taskID, task.TaskID())
}

func (s *historyReplicationTaskSuite) TestNewHistoryReplicationTask() {
	replicationTask := s.getHistoryReplicationTask()
	replicationAttr := replicationTask.HistoryTaskAttributes

	task := newHistoryReplicationTask(replicationTask, s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	// overwrite the logger for easy comparison
	task.logger = s.logger
	s.Equal(
		&historyReplicationTask{
			workflowReplicationTask: workflowReplicationTask{
				metricsScope: metrics.HistoryReplicationTaskScope,
				startTime:    task.startTime,
				partitionID: definition.NewWorkflowIdentifier(
					replicationAttr.GetDomainId(),
					replicationAttr.GetWorkflowId(),
					replicationAttr.GetRunId(),
				),
				taskID:              replicationAttr.GetFirstEventId(),
				attempt:             0,
				kafkaMsg:            s.mockMsg,
				logger:              s.logger,
				config:              s.config,
				historyClient:       s.mockHistoryClient,
				metricsClient:       s.metricsClient,
				historyRereplicator: s.mockRereplicator,
			},
			req: &h.ReplicateEventsRequest{
				SourceCluster: common.StringPtr(s.sourceCluster),
				DomainUUID:    replicationAttr.DomainId,
				WorkflowExecution: &shared.WorkflowExecution{
					WorkflowId: replicationAttr.WorkflowId,
					RunId:      replicationAttr.RunId,
				},
				FirstEventId:            replicationAttr.FirstEventId,
				NextEventId:             replicationAttr.NextEventId,
				Version:                 replicationAttr.Version,
				ReplicationInfo:         replicationAttr.ReplicationInfo,
				History:                 replicationAttr.History,
				NewRunHistory:           replicationAttr.NewRunHistory,
				ForceBufferEvents:       common.BoolPtr(false),
				EventStoreVersion:       replicationAttr.EventStoreVersion,
				NewRunEventStoreVersion: replicationAttr.NewRunEventStoreVersion,
				ResetWorkflow:           replicationAttr.ResetWorkflow,
			},
		},
		task,
	)
}

func (s *historyReplicationTaskSuite) TestExecute() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.On("ReplicateEvents", mock.Anything, task.req).Return(randomErr).Once()
	err := task.Execute()
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_NotEnoughAttempt() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_NotRetryErr() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	randomErr := errors.New("some random error")

	err := task.HandleErr(randomErr)
	s.Equal(randomErr, err)
}

func (s *historyReplicationTaskSuite) TestHandleErr_RetryErr() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	retryErr := &shared.RetryTaskError{
		DomainId:    common.StringPtr(task.partitionID.DomainID),
		WorkflowId:  common.StringPtr(task.partitionID.WorkflowID),
		RunId:       common.StringPtr("other random run ID"),
		NextEventId: common.Int64Ptr(447),
	}

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.partitionID.DomainID, task.partitionID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.partitionID.RunID, task.taskID,
	).Return(errors.New("some random error")).Once()
	err := task.HandleErr(retryErr)
	s.Equal(retryErr, err)

	s.mockRereplicator.On("SendMultiWorkflowHistory",
		task.partitionID.DomainID, task.partitionID.WorkflowID,
		retryErr.GetRunId(), retryErr.GetNextEventId(),
		task.partitionID.RunID, task.taskID,
	).Return(nil).Once()
	s.mockHistoryClient.On("ReplicateEvents", mock.Anything, task.req).Return(nil).Once()
	err = task.HandleErr(retryErr)
	s.Nil(err)
}

func (s *historyReplicationTaskSuite) TestRetryErr_NonRetryable() {
	err := &shared.BadRequestError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable() {
	err := &shared.InternalServiceError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = 0
	s.True(task.RetryErr(err))
	s.False(task.req.GetForceBufferEvents())

	task.attempt = s.config.ReplicatorHistoryBufferRetryCount()
	s.True(task.RetryErr(err))
	s.True(task.req.GetForceBufferEvents())
}

func (s *historyReplicationTaskSuite) TestRetryErr_Retryable_ExceedAttempt() {
	err := &shared.InternalServiceError{}
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)
	task.attempt = s.config.ReplicationTaskMaxRetry() + 100
	s.False(task.RetryErr(err))
}

func (s *historyReplicationTaskSuite) TestAck() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Ack").Return(nil).Once()
	task.Ack()
}

func (s *historyReplicationTaskSuite) TestNack() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.mockMsg.On("Nack").Return(nil).Once()
	task.Nack()
}

func (s *historyReplicationTaskSuite) TestQueueID() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.Equal(task.partitionID, task.PartitionID())
}

func (s *historyReplicationTaskSuite) TestTaskID() {
	task := newHistoryReplicationTask(s.getHistoryReplicationTask(), s.mockMsg, s.sourceCluster, s.logger,
		s.config, s.mockHistoryClient, s.metricsClient, s.mockRereplicator)

	s.Equal(task.taskID, task.TaskID())
}

func (s *activityReplicationTaskSuite) getActivityReplicationTask() *replicator.ReplicationTask {
	replicationAttr := &replicator.SyncActicvityTaskAttributes{
		DomainId:          common.StringPtr("some random domain ID"),
		WorkflowId:        common.StringPtr("some random workflow ID"),
		RunId:             common.StringPtr("some random run ID"),
		Version:           common.Int64Ptr(1394),
		ScheduledId:       common.Int64Ptr(728),
		ScheduledTime:     common.Int64Ptr(time.Now().UnixNano()),
		StartedId:         common.Int64Ptr(1015),
		StartedTime:       common.Int64Ptr(time.Now().UnixNano()),
		LastHeartbeatTime: common.Int64Ptr(time.Now().UnixNano()),
		Details:           []byte("some random detail"),
		Attempt:           common.Int32Ptr(59),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                    replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActicvityTaskAttributes: replicationAttr,
	}
	return replicationTask
}

func (s *historyReplicationTaskSuite) getHistoryReplicationTask() *replicator.ReplicationTask {
	replicationAttr := &replicator.HistoryTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       common.StringPtr("some random domain ID"),
		WorkflowId:     common.StringPtr("some random workflow ID"),
		RunId:          common.StringPtr("some random run ID"),
		Version:        common.Int64Ptr(1394),
		FirstEventId:   common.Int64Ptr(728),
		NextEventId:    common.Int64Ptr(1015),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			cluster.TestCurrentClusterName: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(0644),
				LastEventId: common.Int64Ptr(0755),
			},
			cluster.TestAlternativeClusterName: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(0755),
				LastEventId: common.Int64Ptr(0644),
			},
		},
		History: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(1)}},
		},
		NewRunHistory: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(2)}},
		},
		EventStoreVersion:       common.Int32Ptr(144),
		NewRunEventStoreVersion: common.Int32Ptr(16384),
		ResetWorkflow:           common.BoolPtr(true),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:              replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: replicationAttr,
	}
	return replicationTask
}

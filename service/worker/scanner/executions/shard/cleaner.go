package shard

import (
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

const (
	deleteMaxRetries = 10
)

type (
	cleaner struct {
		shardID int
		failedBufferedWriter util.BufferedWriter
		cleanedBufferedWriter util.BufferedWriter
		skippedBufferedWriter util.BufferedWriter
		checkers []checks.Checker
		checkRequestIterator CheckRequestIterator
		persistenceRetryer util.PersistenceRetryer
	}
)

// NewCleaner constructs a new cleaner
func NewCleaner(
	shardID int,
	failedBufferedWriter util.BufferedWriter,
	cleanedBufferedWriter util.BufferedWriter,
	skippedBufferedWriter util.BufferedWriter,
	checkers []checks.Checker,
	checkRequestIterator CheckRequestIterator,
	persistenceRetryer util.PersistenceRetryer,
) Cleaner {
	return &cleaner{
		shardID: shardID,
		failedBufferedWriter: failedBufferedWriter,
		cleanedBufferedWriter: cleanedBufferedWriter,
		skippedBufferedWriter: skippedBufferedWriter,
		checkers: checkers,
		checkRequestIterator: checkRequestIterator,
		persistenceRetryer: persistenceRetryer,
	}
}

func (c *cleaner) Clean() *CleanReport {
	report := &CleanReport{
		ShardID: c.shardID,
		Handled: &Handled{},
	}
	defer func() {
		if err := c.failedBufferedWriter.Flush(); err != nil {
			report.Failures = append(report.Failures, &CleanFailure{
				Note:    "failed to flush failedBufferedWriter",
				Details: err.Error(),
			})
		}
		if err := c.cleanedBufferedWriter.Flush(); err != nil {
			report.Failures = append(report.Failures, &CleanFailure{
				Note:    "failed to flush cleanedBufferedWriter",
				Details: err.Error(),
			})
		}
		if err := c.skippedBufferedWriter.Flush(); err != nil {
			report.Failures = append(report.Failures, &CleanFailure{
				Note:    "failed to flush skippedBufferedWriter",
				Details: err.Error(),
			})
		}
	}()
	for c.checkRequestIterator.HasNext() {
		curr, err := c.checkRequestIterator.Next()
		if err != nil {
			report.Failures = append(report.Failures, &CleanFailure{
				Note:    "iterator entered an invalid state",
				Details: err.Error(),
			})
			return report
		}
		if curr.Error != nil {
			report.Failures = append(report.Failures, &CleanFailure{
				Note:    "failed to get next from iterator",
				Details: curr.Error.Error(),
			})
			continue
		}
		report.Handled.ExecutionCount++
		resources := &checks.CheckResources{}
		verifiedCorruption := false
		for _, checker := range c.checkers {
			resp := checker.Check(curr.CheckRequest, resources)
			if resp.ResultType == checks.ResultTypeCorrupted {
				verifiedCorruption = true
			}
			if resp.ResultType != checks.ResultTypeHealthy {
				break
			}
		}
		if !verifiedCorruption {
			report.Handled.SkippedCount++
			continue
		}
		deleteConcreteReq := &persistence.DeleteWorkflowExecutionRequest{
			DomainID:   curr.CheckRequest.DomainID,
			WorkflowID: curr.CheckRequest.WorkflowID,
			RunID:      curr.CheckRequest.RunID,
		}
		deletedConcrete := false
		for i := 0; i < deleteMaxRetries; i++ {
			if err := c.persistenceRetryer.DeleteWorkflowExecution(deleteConcreteReq); err == nil {
				deletedConcrete = true
				break
			}
			// TODO: also break out of this loop if its not a retryable persistence error
		}
		if !deletedConcrete {
			// this is a failure
		} else {
			// we successfully deleted concrete
		}

		deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   curr.CheckRequest.DomainID,
			WorkflowID: curr.CheckRequest.WorkflowID,
			RunID:      curr.CheckRequest.RunID,
		}
		// deleting current execution is best effort, the success or failure of the cleanup
		// is determined above based on if the concrete execution could be deleted
		for i := 0; i < deleteMaxRetries; i++ {
			if err := c.persistenceRetryer.DeleteCurrentWorkflowExecution(deleteCurrentReq); err == nil {
				break
			}
		}
	}
	return report
}

// Tasks
// 1. Finish cleaner implementation
// 2. Write unit tests
// 3. Write code which invokes scan and creates combined report
// 4. Include output location in combined report
// 5. Enable filtering by domainID
// 6. Be sure to include delete failure reason in delete output (the delete output should contain more than just the scan copy)
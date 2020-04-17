package shard

import (
	"fmt"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
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
		config *cleanerConfig
	}

	cleanerConfig struct {
		maxRetries int
		confirmCorruptedCount int
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
	config *cleanerConfig,
) Cleaner {
	return &cleaner{
		shardID: shardID,
		failedBufferedWriter: failedBufferedWriter,
		cleanedBufferedWriter: cleanedBufferedWriter,
		skippedBufferedWriter: skippedBufferedWriter,
		checkers: checkers,
		checkRequestIterator: checkRequestIterator,
		persistenceRetryer: persistenceRetryer,
		config: config,
	}
}

func (c *cleaner) Clean() *CleanReport {
	report := &CleanReport{
		ShardID: c.shardID,
	}
	defer func() {
		flushCleanBuffer(c.failedBufferedWriter, report, "failed to flush failedBufferedWriter")
		flushCleanBuffer(c.cleanedBufferedWriter, report, "failed to flush cleanedBufferedWriter")
		flushCleanBuffer(c.skippedBufferedWriter, report, "failed to flush skippedBufferedWriter")
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
		checkResponse := checks.CheckResponse{}
		for _, checker := range c.checkers {
			checkResponse = checker.Check(curr.CheckRequest, resources)
			if checkResponse.ResultType != checks.ResultTypeHealthy {
				break
			}
		}
		scannedRecordedEntity := ScannedRecordedEntity{
			CheckRequest: curr.CheckRequest,
			CheckResponse: checkResponse,
		}
		if checkResponse.ResultType != checks.ResultTypeCorrupted {
			report.Handled.SkippedCount++
			writeToCleanBuffer(
				c.skippedBufferedWriter,
				scannedRecordedEntity,
				CleanResultTypeSkipped,
				"skipped because could not verify corruption",
				"",
				report,
				"failed to write to skippedBufferedWriter")
			continue
		}
		deleteConcreteReq := &persistence.DeleteWorkflowExecutionRequest{
			DomainID:   curr.CheckRequest.DomainID,
			WorkflowID: curr.CheckRequest.WorkflowID,
			RunID:      curr.CheckRequest.RunID,
		}
		var concreteDeleteErr error
		for i := 0; i < c.config.maxRetries; i++ {
			if concreteDeleteErr = c.persistenceRetryer.DeleteWorkflowExecution(deleteConcreteReq); concreteDeleteErr == nil {
				break
			}
			if !common.IsPersistenceTransientError(concreteDeleteErr) {
				break
			}
		}
		deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   curr.CheckRequest.DomainID,
			WorkflowID: curr.CheckRequest.WorkflowID,
			RunID:      curr.CheckRequest.RunID,
		}
		var currentDeleteErr error
		for i := 0; i < c.config.maxRetries; i++ {
			if currentDeleteErr = c.persistenceRetryer.DeleteCurrentWorkflowExecution(deleteCurrentReq); currentDeleteErr == nil {
				break
			}
			if !common.IsPersistenceTransientError(currentDeleteErr) {
				break
			}
		}

		if concreteDeleteErr != nil && currentDeleteErr != nil {
			report.Handled.FailedCount++
			writeToCleanBuffer(
				c.failedBufferedWriter,
				scannedRecordedEntity,
				CleanResultTypeFailed,
				"failed to delete both concrete and current executions",
				fmt.Sprintf("concreteErr: %v, currentErr: %v", concreteDeleteErr, currentDeleteErr),
				report,
				"failed to write to failedBufferedWriter")
		} else if concreteDeleteErr != nil {
			report.Handled.FailedCount++
			writeToCleanBuffer(
				c.failedBufferedWriter,
				scannedRecordedEntity,
				CleanResultTypeFailed,
				"failed to delete concrete execution, but deleted current",
				concreteDeleteErr.Error(),
				report,
				"failed to write to failedBufferedWriter")
		} else if currentDeleteErr != nil {
			report.Handled.FailedCount++
			writeToCleanBuffer(
				c.failedBufferedWriter,
				scannedRecordedEntity,
				CleanResultTypeFailed,
				"failed to delete current execution, but deleted concrete",
				currentDeleteErr.Error(),
				report,
				"failed to write to failedBufferedWriter")
		} else {
			report.Handled.CleanedCount++
			writeToCleanBuffer(
				c.cleanedBufferedWriter,
				scannedRecordedEntity,
				CleanResultTypeCleaned,
				"successfully cleaned both concrete execution and current execution",
				"",
				report,
				"failed to write to cleanedBufferedWriter")
		}
	}
	return report
}

func writeToCleanBuffer(
	buffer util.BufferedWriter,
	scannedRecordedEntity ScannedRecordedEntity,
	cleanResultType CleanResultType,
	cleanNote string,
	cleanDetails string,
	cleanReport *CleanReport,
	writeFailureNote string,
) {
	e := CleanedRecordedEntity{
		ScannedRecordedEntity: scannedRecordedEntity,
		CleanAttemptInfo:      CleanAttemptInfo{
			ResultType: cleanResultType,
			Note: cleanNote,
			Details: cleanDetails,
		},
	}
	if err := buffer.Add(e); err != nil {
		cleanReport.Failures = append(cleanReport.Failures, &CleanFailure{
			Note: writeFailureNote,
			Details: err.Error(),
		})
	}
}

func flushCleanBuffer(
	buffer util.BufferedWriter,
	cleanReport *CleanReport,
	flushFailureNote string,
) {
	if err := buffer.Flush(); err != nil {
		cleanReport.Failures = append(cleanReport.Failures, &CleanFailure{
			Note:    flushFailureNote,
			Details: err.Error(),
		})
	}
}


// Tasks
// 1. Review all code
// 2. Write unit tests
// 3. Write code which invokes scan and creates combined report
// 4. Include output location in combined report
// 5. Enable filtering by domainID
// 6. Be sure to include delete failure reason in delete output (the delete output should contain more than just the scan copy)
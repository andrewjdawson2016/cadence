package shard

import (
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type (
	scanner struct {
		shardID                 int
		failedBufferedWriter    common.BufferedWriter
		corruptedBufferedWriter common.BufferedWriter
		checkers                []checks.Checker
		checkRequestIterator    CheckRequestIterator
	}
)

// NewScanner constructs a new scanner
func NewScanner(
	shardID int,
	failedBufferedWriter common.BufferedWriter,
	corruptedBufferedWriter common.BufferedWriter,
	checkers []checks.Checker,
	checkRequestIterator CheckRequestIterator,
) Scanner {
	return &scanner{
		shardID: shardID,
		failedBufferedWriter: failedBufferedWriter,
		corruptedBufferedWriter: corruptedBufferedWriter,
		checkers: checkers,
		checkRequestIterator: checkRequestIterator,
	}
}

func (s *scanner) Scan() *ScanReport {
	report := &ScanReport{
		ShardID: s.shardID,
		Scanned: Scanned{
			CorruptionByType: make(map[string]int64),
		},
	}
	defer func() {
		flushScanBuffer(s.failedBufferedWriter, report, "failed to flush failedBufferedWriter")
		flushScanBuffer(s.corruptedBufferedWriter, report, "failed to flush corruptedBufferedWriter")
	}()
	for s.checkRequestIterator.HasNext() {
		curr, err := s.checkRequestIterator.Next()
		if err != nil {
			report.Failures = append(report.Failures, &ScanFailure{
				Note:    "iterator entered an invalid state",
				Details: err.Error(),
			})
			return report
		}
		if curr.Error != nil {
			report.Failures = append(report.Failures, &ScanFailure{
				Note:    "failed to get next from iterator",
				Details: curr.Error.Error(),
			})
			continue
		}
		report.Scanned.ExecutionsCount++
		resources := &checks.CheckResources{}
	CheckerLoop:
		for _, checker := range s.checkers {
			resp := checker.Check(curr.CheckRequest, resources)
			switch resp.ResultType {
			case checks.ResultTypeCorrupted:
				report.Scanned.CorruptedCount++
				report.Scanned.CorruptionByType[checker.CheckType()]++
				if checks.ExecutionOpen(curr.CheckRequest) {
					report.Scanned.CorruptOpenCount++
				}
				writeToScanBuffer(s.corruptedBufferedWriter, curr.CheckRequest, resp, report, "failed to add to corruptedBufferedWriter")
			case checks.ResultTypeFailed:
				report.Scanned.CheckFailedCount++
				writeToScanBuffer(s.failedBufferedWriter, curr.CheckRequest, resp, report, "failed to add to failedBufferedWriter")
			}
			if resp.ResultType != checks.ResultTypeHealthy {
				break CheckerLoop
			}
		}
	}
	return report
}

func writeToScanBuffer(
	buffer common.BufferedWriter,
	checkRequest checks.CheckRequest,
	checkResponse checks.CheckResponse,
	scanReport *ScanReport,
	writeFailureNote string,
) {
	e := ScannedRecordedEntity{
		CheckRequest:  checkRequest,
		CheckResponse: checkResponse,
	}
	if err := buffer.Add(e); err != nil {
		scanReport.Failures = append(scanReport.Failures, &ScanFailure{
			Note: writeFailureNote,
			Details: err.Error(),
		})
	}
}

func flushScanBuffer(
	buffer common.BufferedWriter,
	scanReport *ScanReport,
	flushFailureNote string,
) {
	if err := buffer.Flush(); err != nil {
		scanReport.Failures = append(scanReport.Failures, &ScanFailure{
			Note:    flushFailureNote,
			Details: err.Error(),
		})
	}
}


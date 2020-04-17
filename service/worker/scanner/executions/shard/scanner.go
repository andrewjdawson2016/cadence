package shard

import (
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

type (
	scanner struct {
		shardID int
		failedBufferedWriter util.BufferedWriter
		corruptedBufferedWriter util.BufferedWriter
		checkers []checks.Checker
		checkRequestIterator CheckRequestIterator
	}
)

// NewScanner constructs a new scanner
func NewScanner(
	shardID int,
	failedBufferedWriter util.BufferedWriter,
	corruptedBufferedWriter util.BufferedWriter,
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
		Scanned: &Scanned{
			CorruptionByType: make(map[string]int64),
		},
	}
	defer func() {
		if err := s.failedBufferedWriter.Flush(); err != nil {
			report.Failures = append(report.Failures, &ScanFailure{
				Note:    "failed to flush failedBufferedWriter",
				Details: err.Error(),
			})
		}
		if err := s.corruptedBufferedWriter.Flush(); err != nil {
			report.Failures = append(report.Failures, &ScanFailure{
				Note:    "failed to flush corruptedBufferedWriter",
				Details: err.Error(),
			})
		}
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
			se := &ScannedRecordedEntity{
				CheckRequest:  curr.CheckRequest,
				CheckResponse: resp,
			}
			switch resp.ResultType {
			case checks.ResultTypeCorrupted:
				report.Scanned.CorruptedCount++
				report.Scanned.CorruptionByType[checker.CheckType()]++
				if checks.ExecutionOpen(curr.CheckRequest) {
					report.Scanned.CorruptOpenCount++
				}
				if err := s.corruptedBufferedWriter.Add(se); err != nil {
					report.Failures = append(report.Failures, &ScanFailure{
						Note: "failed to add to corruptedBufferedWriter",
						Details: err.Error(),
					})
				}
			case checks.ResultTypeFailed:
				report.Scanned.CheckFailedCount++
				if err := s.failedBufferedWriter.Add(se); err != nil {
					report.Failures = append(report.Failures, &ScanFailure{
						Note: "failed to add to failedBufferedWriter",
						Details: err.Error(),
					})
				}
			}
			if resp.ResultType != checks.ResultTypeHealthy {
				break CheckerLoop
			}
		}
	}
	return report
}


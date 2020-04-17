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
		persistenceRetryer util.PersistenceRetryer
		checkers []checks.Checker
		checkRequestIterator CheckRequestIterator
	}
)

func NewScanner(
	shardID int,
	failedBufferedWriter util.BufferedWriter,
	corruptedBufferedWriter util.BufferedWriter,
	persistenceRetryer util.PersistenceRetryer,
	checkers []checks.Checker,
	checkRequestIterator CheckRequestIterator,
) Scanner {
	return &scanner{
		shardID: shardID,
		failedBufferedWriter: failedBufferedWriter,
		corruptedBufferedWriter: corruptedBufferedWriter,
		persistenceRetryer: persistenceRetryer,
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
			report.ScanFailures = append(report.ScanFailures, &ScanFailure{
				Note:    "failed to flush failedBufferedWriter",
				Details: err.Error(),
			})
		}
		if err := s.corruptedBufferedWriter.Flush(); err != nil {
			report.ScanFailures = append(report.ScanFailures, &ScanFailure{
				Note:    "failed to flush corruptedBufferedWriter",
				Details: err.Error(),
			})
		}
	}()
	for s.checkRequestIterator.HasNext() {
		curr, err := s.checkRequestIterator.Next()
		if err != nil {
			report.ScanFailures = append(report.ScanFailures, &ScanFailure{
				Note:    "iterator entered an invalid state",
				Details: err.Error(),
			})
			return report
		}
		if curr.Error != nil {
			report.ScanFailures = append(report.ScanFailures, &ScanFailure{
				Note:    "failed to get next check request from iterator",
				Details: curr.Error.Error(),
			})
			continue
		}
		report.Scanned.ExecutionsCount++
		for _, c := range s.checkers {
			resp := c.Check(curr.CheckRequest)
			switch resp.ResultType {
			case checks.ResultTypeHealthy:
			case checks.ResultTypeCorrupted:
				report.Scanned.CorruptedCount++
				report.Scanned.CorruptionByType[c.CheckType()]++
				// TODO: need util function here to determine if its open then update open and confirm I updated everything in shard report correctly
			case checks.ResultTypeFailed:
				report.Scanned.CheckFailedCount++
			}
		}
	}
}


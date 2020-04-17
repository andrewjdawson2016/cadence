package shard

import (
	"errors"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/checks"
	"github.com/uber/cadence/service/worker/scanner/executions/util"
)

const (
	maxRetries = 10
)

type (
	persistenceCheckRequestIterator struct {
		shardID            int
		persistenceRetryer util.PersistenceRetryer
		pageSize           int
		payloadSerializer  persistence.PayloadSerializer
		branchDecoder      *codec.ThriftRWEncoder

		// the following keep track of iterator's state
		currPage      *persistence.InternalListConcreteExecutionsResponse
		currPageIndex int
		nextResult    *CheckRequestIteratorResult
		nextError     error
		hasFetchedFirstPage bool
	}
)

// NewPersistenceCheckRequestIterator constructs a persistenceCheckRequestIterator
func NewPersistenceCheckRequestIterator(
	shardID int,
	persistenceRetryer util.PersistenceRetryer,
	pageSize int,
	payloadSerializer persistence.PayloadSerializer,
	branchDecoder *codec.ThriftRWEncoder,
) CheckRequestIterator {
	itr := &persistenceCheckRequestIterator{
		shardID:            shardID,
		persistenceRetryer: persistenceRetryer,
		pageSize:           pageSize,
		payloadSerializer:  payloadSerializer,
		branchDecoder:      branchDecoder,

		currPage:      &persistence.InternalListConcreteExecutionsResponse{},
		currPageIndex: -1,
		nextResult:    nil,
		nextError:     nil,
		hasFetchedFirstPage: false,
	}
	itr.advance()
	return itr
}

func (itr *persistenceCheckRequestIterator) Next() (*CheckRequestIteratorResult, error) {
	currResult := itr.nextResult
	currErr := itr.nextError
	if itr.HasNext() {
		itr.advance()
	}
	return currResult, currErr
}

func (itr *persistenceCheckRequestIterator) HasNext() bool {
	return itr.nextResult != nil
}

func (itr *persistenceCheckRequestIterator) advance() {
	itr.currPageIndex++
	currPageResult := itr.getFromCurrentPage()
	if currPageResult != nil {
		itr.nextResult = currPageResult
		itr.nextError = nil
		return
	}
	attemptFetchFirstPage := len(itr.currPage.NextPageToken) == 0
	if attemptFetchFirstPage && itr.hasFetchedFirstPage {
		itr.nextResult = nil
		itr.nextError = ErrIteratorEmpty
		return
	}
	page, err := itr.getNextPage()
	if err != nil {
		itr.nextResult = nil
		itr.nextError = err
		return
	}
	itr.hasFetchedFirstPage = true
	itr.currPage = page
	itr.currPageIndex = 0
	currPageResult = itr.getFromCurrentPage()
	if currPageResult != nil {
		itr.nextResult = currPageResult
		itr.nextError = nil
		return
	}
	itr.nextResult = nil
	itr.nextError = errors.New("fetched new page from persistence but still could not get next response")
}

func (itr *persistenceCheckRequestIterator) getFromCurrentPage() *CheckRequestIteratorResult {
	if itr.currPageIndex >= len(itr.currPage.Executions) {
		return nil
	}
	cr, err := itr.convertListEntityToCheckRequest(itr.currPage.Executions[itr.currPageIndex])
	return &CheckRequestIteratorResult{
		CheckRequest: cr,
		Error:        err,
	}
}

func (itr *persistenceCheckRequestIterator) getNextPage() (*persistence.InternalListConcreteExecutionsResponse, error) {
	req := &persistence.ListConcreteExecutionsRequest{
		PageSize:  itr.pageSize,
		PageToken: itr.currPage.NextPageToken,
	}
	var resp *persistence.InternalListConcreteExecutionsResponse
	var err error
	for i := 0; i < maxRetries; i++ {
		resp, err = itr.persistenceRetryer.ListConcreteExecutions(req)
		if err == nil {
			return resp, nil
		}
		if !common.IsPersistenceTransientError(err) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (itr *persistenceCheckRequestIterator) convertListEntityToCheckRequest(e *persistence.InternalListConcreteExecutionsEntity) (*checks.CheckRequest, error) {
	hb, err := itr.historyBranch(e)
	if err != nil {
		return nil, err
	}
	return &checks.CheckRequest{
		ShardID:    itr.shardID,
		DomainID:   e.ExecutionInfo.DomainID,
		WorkflowID: e.ExecutionInfo.WorkflowID,
		RunID:      e.ExecutionInfo.RunID,
		TreeID:     hb.GetTreeID(),
		BranchID:   hb.GetBranchID(),
		State:      e.ExecutionInfo.State,
	}, nil
}

func (itr *persistenceCheckRequestIterator) historyBranch(e *persistence.InternalListConcreteExecutionsEntity) (*shared.HistoryBranch, error) {
	branchTokenBytes := e.ExecutionInfo.BranchToken
	if len(branchTokenBytes) == 0 {
		if e.VersionHistories == nil {
			return nil, errors.New("failed to get branch token")
		}
		vh, err := itr.payloadSerializer.DeserializeVersionHistories(e.VersionHistories)
		if err != nil {
			return nil, err
		}
		branchTokenBytes = vh.GetHistories()[vh.GetCurrentVersionHistoryIndex()].GetBranchToken()
	}
	var branch shared.HistoryBranch
	if err := itr.branchDecoder.Decode(branchTokenBytes, &branch); err != nil {
		return nil, err
	}
	return &branch, nil
}

// The MIT License (MIT)
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package util

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
)

type (
	// PersistenceRetryer is used to retry and rate limit requests to util
	PersistenceRetryer interface {
		ListConcreteExecutions(*persistence.ListConcreteExecutionsRequest) (*persistence.InternalListConcreteExecutionsResponse, error)
		GetWorkflowExecution(*persistence.GetWorkflowExecutionRequest) (*persistence.InternalGetWorkflowExecutionResponse, error)
		GetCurrentExecution(*persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error)
		ReadHistoryBranch(*persistence.InternalReadHistoryBranchRequest) (*persistence.InternalReadHistoryBranchResponse, error)
		DeleteWorkflowExecution(*persistence.DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *persistence.DeleteCurrentWorkflowExecutionRequest) error
	}

	persistenceRetryer struct {
		limiter quotas.Limiter
		execStore persistence.ExecutionStore
		historyStore persistence.HistoryStore
	}
)

var (
	retryPolicy = common.CreatePersistanceRetryPolicy()
)

// NewPersistenceRetryer constructs a new PersistenceRetryer
func NewPersistenceRetryer(
	limiter quotas.Limiter,
	execStore persistence.ExecutionStore,
	historyStore persistence.HistoryStore,
) PersistenceRetryer {
	return &persistenceRetryer{
		limiter: limiter,
		execStore: execStore,
		historyStore: historyStore,
	}
}

// ListConcreteExecutions retries ListConcreteExecutions
func (pr *persistenceRetryer) ListConcreteExecutions(
	req *persistence.ListConcreteExecutionsRequest,
) (*persistence.InternalListConcreteExecutionsResponse, error) {
	var resp *persistence.InternalListConcreteExecutionsResponse
	op := func() error {
		var err error
		pr.limiter.Wait(context.Background())
		resp, err = pr.execStore.ListConcreteExecutions(req)
		return err
	}
	var err error
	err = backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

// GetWorkflowExecution retries GetWorkflowExecution
func (pr *persistenceRetryer) GetWorkflowExecution(
	req *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	var resp *persistence.InternalGetWorkflowExecutionResponse
	op := func() error {
		var err error
		pr.limiter.Wait(context.Background())
		resp, err = pr.execStore.GetWorkflowExecution(req)
		return err
	}
	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetCurrentExecution retries GetCurrentExecution
func (pr *persistenceRetryer) GetCurrentExecution(
	req *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	var resp *persistence.GetCurrentExecutionResponse
	op := func() error {
		var err error
		pr.limiter.Wait(context.Background())
		resp, err = pr.execStore.GetCurrentExecution(req)
		return err
	}
	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReadHistoryBranch retries ReadHistoryBranch
func (pr *persistenceRetryer) ReadHistoryBranch(
	req *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	var resp *persistence.InternalReadHistoryBranchResponse
	op := func() error {
		var err error
		pr.limiter.Wait(context.Background())
		resp, err = pr.historyStore.ReadHistoryBranch(req)
		return err
	}

	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteWorkflowExecution retries DeleteWorkflowExecution
func (pr *persistenceRetryer) DeleteWorkflowExecution(
	req *persistence.DeleteWorkflowExecutionRequest,
) error {
	op := func() error {
		pr.limiter.Wait(context.Background())
		return pr.execStore.DeleteWorkflowExecution(req)
	}
	return backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
}

// DeleteCurrentWorkflowExecution retries DeleteCurrentWorkflowExecution
func (pr *persistenceRetryer) DeleteCurrentWorkflowExecution(
	req *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	op := func() error {
		pr.limiter.Wait(context.Background())
		return pr.execStore.DeleteCurrentWorkflowExecution(req)
	}
	return backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
}

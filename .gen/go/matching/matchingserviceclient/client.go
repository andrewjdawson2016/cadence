// The MIT License (MIT)
// 
// Copyright (c) 2019 Uber Technologies, Inc.
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

// Code generated by thriftrw-plugin-yarpc
// @generated

package matchingserviceclient

import (
	context "context"
	matching "github.com/uber/cadence/.gen/go/matching"
	shared "github.com/uber/cadence/.gen/go/shared"
	wire "go.uber.org/thriftrw/wire"
	yarpc "go.uber.org/yarpc"
	transport "go.uber.org/yarpc/api/transport"
	thrift "go.uber.org/yarpc/encoding/thrift"
	reflect "reflect"
)

// Interface is a client for the MatchingService service.
type Interface interface {
	AddActivityTask(
		ctx context.Context,
		AddRequest *matching.AddActivityTaskRequest,
		opts ...yarpc.CallOption,
	) error

	AddDecisionTask(
		ctx context.Context,
		AddRequest *matching.AddDecisionTaskRequest,
		opts ...yarpc.CallOption,
	) error

	AddInMemoryDecisionTask(
		ctx context.Context,
		AddRequest *matching.AddInMemoryDecisionTaskRequest,
		opts ...yarpc.CallOption,
	) error

	CancelOutstandingPoll(
		ctx context.Context,
		Request *matching.CancelOutstandingPollRequest,
		opts ...yarpc.CallOption,
	) error

	DescribeTaskList(
		ctx context.Context,
		Request *matching.DescribeTaskListRequest,
		opts ...yarpc.CallOption,
	) (*shared.DescribeTaskListResponse, error)

	PollForActivityTask(
		ctx context.Context,
		PollRequest *matching.PollForActivityTaskRequest,
		opts ...yarpc.CallOption,
	) (*shared.PollForActivityTaskResponse, error)

	PollForDecisionTask(
		ctx context.Context,
		PollRequest *matching.PollForDecisionTaskRequest,
		opts ...yarpc.CallOption,
	) (*matching.PollForDecisionTaskResponse, error)

	QueryWorkflow(
		ctx context.Context,
		QueryRequest *matching.QueryWorkflowRequest,
		opts ...yarpc.CallOption,
	) (*shared.QueryWorkflowResponse, error)

	RespondQueryTaskCompleted(
		ctx context.Context,
		Request *matching.RespondQueryTaskCompletedRequest,
		opts ...yarpc.CallOption,
	) error
}

// New builds a new client for the MatchingService service.
//
// 	client := matchingserviceclient.New(dispatcher.ClientConfig("matchingservice"))
func New(c transport.ClientConfig, opts ...thrift.ClientOption) Interface {
	return client{
		c: thrift.New(thrift.Config{
			Service:      "MatchingService",
			ClientConfig: c,
		}, opts...),
	}
}

func init() {
	yarpc.RegisterClientBuilder(
		func(c transport.ClientConfig, f reflect.StructField) Interface {
			return New(c, thrift.ClientBuilderOptions(c, f)...)
		},
	)
}

type client struct {
	c thrift.Client
}

func (c client) AddActivityTask(
	ctx context.Context,
	_AddRequest *matching.AddActivityTaskRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := matching.MatchingService_AddActivityTask_Helper.Args(_AddRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_AddActivityTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = matching.MatchingService_AddActivityTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) AddDecisionTask(
	ctx context.Context,
	_AddRequest *matching.AddDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := matching.MatchingService_AddDecisionTask_Helper.Args(_AddRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_AddDecisionTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = matching.MatchingService_AddDecisionTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) AddInMemoryDecisionTask(
	ctx context.Context,
	_AddRequest *matching.AddInMemoryDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := matching.MatchingService_AddInMemoryDecisionTask_Helper.Args(_AddRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_AddInMemoryDecisionTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = matching.MatchingService_AddInMemoryDecisionTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) CancelOutstandingPoll(
	ctx context.Context,
	_Request *matching.CancelOutstandingPollRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := matching.MatchingService_CancelOutstandingPoll_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_CancelOutstandingPoll_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = matching.MatchingService_CancelOutstandingPoll_Helper.UnwrapResponse(&result)
	return
}

func (c client) DescribeTaskList(
	ctx context.Context,
	_Request *matching.DescribeTaskListRequest,
	opts ...yarpc.CallOption,
) (success *shared.DescribeTaskListResponse, err error) {

	args := matching.MatchingService_DescribeTaskList_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_DescribeTaskList_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = matching.MatchingService_DescribeTaskList_Helper.UnwrapResponse(&result)
	return
}

func (c client) PollForActivityTask(
	ctx context.Context,
	_PollRequest *matching.PollForActivityTaskRequest,
	opts ...yarpc.CallOption,
) (success *shared.PollForActivityTaskResponse, err error) {

	args := matching.MatchingService_PollForActivityTask_Helper.Args(_PollRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_PollForActivityTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = matching.MatchingService_PollForActivityTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) PollForDecisionTask(
	ctx context.Context,
	_PollRequest *matching.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption,
) (success *matching.PollForDecisionTaskResponse, err error) {

	args := matching.MatchingService_PollForDecisionTask_Helper.Args(_PollRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_PollForDecisionTask_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = matching.MatchingService_PollForDecisionTask_Helper.UnwrapResponse(&result)
	return
}

func (c client) QueryWorkflow(
	ctx context.Context,
	_QueryRequest *matching.QueryWorkflowRequest,
	opts ...yarpc.CallOption,
) (success *shared.QueryWorkflowResponse, err error) {

	args := matching.MatchingService_QueryWorkflow_Helper.Args(_QueryRequest)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_QueryWorkflow_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	success, err = matching.MatchingService_QueryWorkflow_Helper.UnwrapResponse(&result)
	return
}

func (c client) RespondQueryTaskCompleted(
	ctx context.Context,
	_Request *matching.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption,
) (err error) {

	args := matching.MatchingService_RespondQueryTaskCompleted_Helper.Args(_Request)

	var body wire.Value
	body, err = c.c.Call(ctx, args, opts...)
	if err != nil {
		return
	}

	var result matching.MatchingService_RespondQueryTaskCompleted_Result
	if err = result.FromWire(body); err != nil {
		return
	}

	err = matching.MatchingService_RespondQueryTaskCompleted_Helper.UnwrapResponse(&result)
	return
}

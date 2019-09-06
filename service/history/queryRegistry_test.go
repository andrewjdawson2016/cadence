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

package history

import (
	"github.com/uber/cadence/.gen/go/shared"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type QueryRegistrySuite struct {
	*require.Assertions
	suite.Suite
}

func TestQueryRegistrySuite(t *testing.T) {
	suite.Run(t, new(QueryRegistrySuite))
}

func (s *QueryRegistrySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *QueryRegistrySuite) TestQueryRegistry() {
	qr := NewQueryRegistry()
	var queries []Query
	for i := 0; i < 10; i++ {
		queries = append(queries, qr.BufferQuery(&shared.WorkflowQuery{}))
	}

	q, err := qr.GetQuery(queries[0].ID())
	s.NoError(err)
	s.NotNil(q)
	q, err = qr.GetQuery("not_exists")
	s.Equal(errQueryNotFound, err)
	s.Nil(q)

	for i := 0; i < 5; i++ {
		changed, err := queries[i].RecordEvent(QueryEventStart, nil)
		s.True(changed)
		s.NoError(err)
	}
	startedQueries, err := qr.StartBuffered()
	s.NoError(err)
	s.Len(startedQueries, 5)

	startedQueries, err = qr.StartBuffered()
	s.NoError(err)
	s.Len(startedQueries, 0)

	for i := 0; i < 5; i++ {
		changed, err := queries[i].RecordEvent(QueryEventRebuffer, nil)
		s.True(changed)
		s.NoError(err)
	}

	completeQuery := func(q Query) {
		if q.State() == QueryStateBuffered {
			changed, err := q.RecordEvent(QueryEventStart, nil)
			s.True(changed)
			s.NoError(err)
		}
		changed, err := q.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
		s.False(changed)
		s.NoError(err)
		changed, err = q.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
		s.True(changed)
		s.NoError(err)
		s.Equal(QueryStateCompleted, q.State())
	}

	expireQuery := func(q Query) {
		changed, err := q.RecordEvent(QueryEventExpire, nil)
		s.True(changed)
		s.NoError(err)
	}

	q0, err := qr.GetQuery(queries[0].ID())
	s.NoError(err)
	s.NotNil(q0)
	s.Equal(QueryStateBuffered, q0.State())
	completeQuery(q0)
	s.Equal(QueryStateCompleted, q0.State())
	q0, err = qr.GetQuery(queries[0].ID())
	s.Nil(q0)
	s.Equal(errQueryNotFound, err)

	q1, err := qr.GetQuery(queries[1].ID())
	s.NoError(err)
	s.NotNil(q1)
	s.Equal(QueryStateBuffered, q1.State())
	expireQuery(q1)
	s.Equal(QueryStateExpired, q1.State())
	q1, err = qr.GetQuery(queries[1].ID())
	s.Nil(q1)
	s.Equal(errQueryNotFound, err)

	q9, err := qr.GetQuery(queries[9].ID())
	s.NoError(err)
	s.NotNil(q9)
	s.Equal(QueryStateStarted, q9.State())
	completeQuery(q9)
	s.Equal(QueryStateCompleted, q9.State())
	q9, err = qr.GetQuery(queries[9].ID())
	s.Nil(q9)
	s.Equal(errQueryNotFound, err)

	q8, err := qr.GetQuery(queries[8].ID())
	s.NoError(err)
	s.NotNil(q8)
	s.Equal(QueryStateStarted, q8.State())
	expireQuery(q8)
	s.Equal(QueryStateExpired, q8.State())
	q8, err = qr.GetQuery(queries[8].ID())
	s.Nil(q8)
	s.Equal(errQueryNotFound, err)
}

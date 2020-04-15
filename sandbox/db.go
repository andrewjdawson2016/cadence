package main

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/uber/cadence/sandbox/backoff"
	"github.com/uber/cadence/sandbox/quotas"
	"time"
)

const (
	host = "127.0.0.1"
	port = 8042
	username = ""
	password = ""
	keyspace = ""
	cqlProtoVersion = 4
	consistency = gocql.LocalQuorum
	serialConsistency = gocql.LocalSerial
	connectionTimeout = 10 * time.Second
	maxConnections = 20
	maxDBRetries = 10

	query = `SELECT writetime(execution) FROM executions WHERE shard_id = ? and type = 1`
)

func retryList(
	limiter *quotas.RateLimiter,
	totalDBRequests *int64,
	shardID int,
	pageSize int,
	pageToken []byte,
	session *gocql.Session,
) ([]int, []byte, error) {
	var writeTimes []int
	var nextPageToken []byte
	var err error
	op := func() error {
		var err error
		preconditionForDBCall(totalDBRequests, limiter)
		writeTimes, nextPageToken, err = list(shardID, pageSize, pageToken, session)
		return err
	}

	for i := 0; i < maxDBRetries; i++ {
		err = backoff.Retry(op, CreatePersistanceRetryPolicy(), IsPersistenceTransientError)
		if err == nil {
			return writeTimes, nextPageToken, nil
		}
	}
	return nil, nil, err
}

func list(
	shardID int,
	pageSize int,
	pageToken []byte,
	session *gocql.Session,
) ([]int, []byte, error) {
	query := session.Query(query, shardID).PageSize(pageSize).PageState(pageToken)
	iter := query.Iter()
	if iter == nil {
		panic("failed to create iterator")
	}

	var writeTimes []int
	var writeTime int
	for iter.Scan(&writeTime) {
		writeTimes = append(writeTimes, writeTime)
	}

	if err := iter.Close(); err != nil {
		return nil, nil, err
	}
	return writeTimes, iter.PageState(), nil
}

func preconditionForDBCall(totalDBRequests *int64, limiter *quotas.RateLimiter) {
	*totalDBRequests = *totalDBRequests + 1
	limiter.Wait(context.Background())
}

func connectToCassandra() *gocql.Session {
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Keyspace = keyspace
	cluster.NumConns = maxConnections
	cluster.Timeout = connectionTimeout
	cluster.ProtoVersion = cqlProtoVersion
	cluster.Consistency = consistency
	cluster.SerialConsistency = serialConsistency


	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	return session
}
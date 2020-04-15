package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/uber/cadence/sandbox/quotas"
)


func main() {
	session := connectToCassandra()
	rateLimiter := quotas.NewSimpleRateLimiter(1000)
	totalDBRequests := int64(0)
	shardResults := make(map[int][]int)
	for i := 200; i < 400; i++ {
		if i % 20 == 0 {
			fmt.Println("starting handling of shard: ", i)
		}
		writeTimes, err := scanShard(rateLimiter, &totalDBRequests, i, session)
		if err != nil {
			continue
		}
		shardResults[i] = writeTimes
	}

	var smallShards []int
	fmt.Println("total shards finished: ", len(shardResults))
	for k, v := range shardResults {
		if len(v) < 1500 {
			fmt.Printf("shard: %v has %v\n", k, len(v))
			smallShards = append(smallShards, k)
		}
	}
}

func scanShard(
	rateLimiter *quotas.RateLimiter,
	totalDBRequests *int64,
	shardID int,
	session *gocql.Session,
) ([]int, error) {
	var pageToken []byte
	firstIteration := true
	var result []int
	var writeTimes []int
	var err error
	for firstIteration || len(pageToken) > 0 {
		firstIteration = false
		writeTimes, pageToken, err = retryList(rateLimiter, totalDBRequests, shardID, 500, pageToken, session)
		if err != nil {
			return nil, err
		}
		result = append(result, writeTimes...)
	}
	return result, nil
}










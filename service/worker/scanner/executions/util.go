// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
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

package executions

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/workflow"
)

func validateShards(shards Shards) error {
	if shards.List == nil && shards.Range == nil {
		return errors.New("must provide either List or Range")
	}
	if shards.List != nil && shards.Range != nil {
		return errors.New("only one of List or Range can be provided")
	}
	if shards.List != nil && len(shards.List) == 0 {
		return errors.New("empty List provided")
	}
	if shards.Range != nil && shards.Range.Max <= shards.Range.Min {
		return errors.New("empty Range provided")
	}
	return nil
}

func flattenShards(shards Shards) ([]int, int, int) {
	shardList := shards.List
	if len(shardList) == 0 {
		shardList = []int{}
		for i := shards.Range.Min; i < shards.Range.Max; i++ {
			shardList = append(shardList, i)
		}
	}
	min := shardList[0]
	max := shardList[0]
	for i := 1; i < len(shardList); i++ {
		if shardList[i] < min {
			min = shardList[i]
		}
		if shardList[i] > max {
			max = shardList[i]
		}
	}
	return shardList, min, max
}

func resolveFixerConfig(overwrites FixerWorkflowConfigOverwrites) ResolvedFixerWorkflowConfig {
	resolvedConfig := ResolvedFixerWorkflowConfig{
		Concurrency:             25,
		BlobstoreFlushThreshold: 1000,
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: true,
			InvariantCollectionHistory:      true,
		},
	}
	if overwrites.Concurrency != nil {
		resolvedConfig.Concurrency = *overwrites.Concurrency
	}
	if overwrites.BlobstoreFlushThreshold != nil {
		resolvedConfig.BlobstoreFlushThreshold = *overwrites.BlobstoreFlushThreshold
	}
	if overwrites.InvariantCollections != nil {
		resolvedConfig.InvariantCollections = *overwrites.InvariantCollections
	}
	return resolvedConfig
}

func getShortActivityContext(ctx workflow.Context) workflow.Context {
	activityOptions := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1.7,
			ExpirationInterval: 5 * time.Minute,
		},
	}
	return workflow.WithActivityOptions(ctx, activityOptions)
}

func getLongActivityContext(ctx workflow.Context) workflow.Context {
	activityOptions := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    8 * time.Hour,
		HeartbeatTimeout:       time.Minute,
		RetryPolicy: &cadence.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1.7,
			ExpirationInterval: 8 * time.Hour,
		},
	}
	return workflow.WithActivityOptions(ctx, activityOptions)
}

func shardInBounds(minShardID, maxShardID, shardID int) error {
	if shardID > maxShardID || shardID < minShardID {
		return fmt.Errorf("requested shard %v is outside of bounds (min: %v and max: %v)", shardID, minShardID, maxShardID)
	}
	return nil
}
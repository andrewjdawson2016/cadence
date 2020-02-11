package main

import (
	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"testing"
	"time"
)

func BenchmarkSetMaxPacketSize(b *testing.B) {
	r, err := m3.NewReporter(m3.Options{
		HostPorts:                   []string{"127.0.0.1:9052"},
		Service:                     "test-service",
		Env:                         "development",
		MaxPacketSizeBytes:          32000,
	})
	if err != nil {
		panic(err)
	}
	defer r.Close()

	scopeOpts := tally.ScopeOptions{
		Tags:           map[string]string{"common_key_1": "common_value_1", "common_key_2": "common_value_2", "common_key_3": "common_value_3", "common_key_4": "common_value_4"},
		CachedReporter: r,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		dynamicTag := uuid.New()
		dynamicTaggedScope := scope.Tagged(map[string]string{dynamicTag: dynamicTag})
		dynamicTaggedScope.Counter("test_counter").Inc(1)
	}
}

func BenchmarkNotSetMaxPacketSize(b *testing.B) {
	r, err := m3.NewReporter(m3.Options{
		HostPorts:                   []string{"127.0.0.1:9052"},
		Service:                     "test-service",
		Env:                         "development",
	})
	if err != nil {
		panic(err)
	}
	defer r.Close()

	scopeOpts := tally.ScopeOptions{
		Tags:           map[string]string{"common_key_1": "common_value_1", "common_key_2": "common_value_2", "common_key_3": "common_value_3", "common_key_4": "common_value_4"},
		CachedReporter: r,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		dynamicTag := uuid.New()
		dynamicTaggedScope := scope.Tagged(map[string]string{dynamicTag: dynamicTag})
		dynamicTaggedScope.Counter("test_counter").Inc(1)
	}
}
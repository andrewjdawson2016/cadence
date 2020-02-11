package main

import (
	"log"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
)

func main() {
	m3config := m3.Configuration{
		HostPort:    "127.0.0.1:9052",
		Service:     "my_service",
		IncludeHost: true,
		Env:         "production",
	}

	reporter, err := m3config.NewReporter()
	if err != nil {
		log.Fatalf("could not create tally m3 reporter: %v", err)
	}

	rootScope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: reporter,
	}, 1*time.Second)
	defer closer.Close()

	go emitMetrics(rootScope, 1*time.Second)

	select {}

}

func emitMetrics(s tally.Scope, interval time.Duration) {
	tic := time.NewTicker(interval)
	for range tic.C {
		s.Tagged(map[string]string{"app": "codelab"}).Counter("example_counter").Inc(1)
	}
}
package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	_ "net/http/pprof"
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

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func emitMetrics(s tally.Scope, interval time.Duration) {
	tic := time.NewTicker(interval)
	i := 0
	for range tic.C {
		if i % 100 == 0 {
			PrintMemUsage()
		}
		s.Tagged(map[string]string{"app": "codelab"}).Counter("example_counter").Inc(1)
		i++
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
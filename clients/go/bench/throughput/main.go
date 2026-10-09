// Throughput with and without ring-aware routing: 64 goroutines calling Has
// for 10 s each way, against KURWA_NODES. Routing does not shorten one
// request's latency - a quorum read waits for one remote reply either way -
// but it halves the replica requests a read sends, which shows under load.
//
//	KURWA_NODES=127.0.0.1:26501 KURWA_PASSWORD=... go run ./bench/throughput
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kurwadb "github.com/holooloo/kurwadb/clients/go"
)

func run(nodes []string, password string, routing bool) float64 {
	ctx := context.Background()
	db, err := kurwadb.Connect(ctx, kurwadb.Options{Nodes: nodes, Password: password, NoRouting: !routing, PoolSize: 8})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for i := 0; i < 1000; i++ {
		_ = db.Add(ctx, fmt.Sprintf("tp-%d", i))
	}
	var ops atomic.Int64
	deadline := time.Now().Add(10 * time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				if _, err := db.Has(ctx, fmt.Sprintf("tp-%d", (g*7919+i)%1000)); err == nil {
					ops.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	return float64(ops.Load()) / 10
}

func main() {
	nodes := strings.Split(os.Getenv("KURWA_NODES"), ",")
	password := os.Getenv("KURWA_PASSWORD")
	for _, routing := range []bool{false, true, false, true} {
		fmt.Printf("routing %-5v %8.0f Has/s\n", routing, run(nodes, password, routing))
	}
}

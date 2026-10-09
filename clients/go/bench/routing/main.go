// Latency of Has with routing to the key's replica, against without it:
// sequential requests, median and p99 in microseconds.
//
//	KURWA_NODES=10.10.10.112:26379 KURWA_PASSWORD=... go run ./bench/routing [requests]
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	kurwadb "github.com/holooloo/kurwadb/clients/go"
)

func measure(nodes []string, password string, routing bool, count int) (float64, float64) {
	ctx := context.Background()
	db, err := kurwadb.Connect(ctx, kurwadb.Options{Nodes: nodes, Password: password, NoRouting: !routing})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	keys := make([]string, 500)
	for i := range keys {
		keys[i] = "bench:routing:" + strconv.Itoa(i)
		if err := db.Add(ctx, keys[i]); err != nil {
			panic(err)
		}
	}
	for i := 0; i < 500; i++ {
		_, _ = db.Has(ctx, keys[i%len(keys)])
	}
	times := make([]float64, count)
	for i := range times {
		t := time.Now()
		if _, err := db.Has(ctx, keys[i%len(keys)]); err != nil {
			panic(err)
		}
		times[i] = float64(time.Since(t).Microseconds())
	}
	sort.Float64s(times)
	return times[len(times)/2], times[len(times)*99/100]
}

func main() {
	nodes := strings.Split(envOr("KURWA_NODES", "127.0.0.1:6379"), ",")
	count := 3000
	if len(os.Args) > 1 {
		count, _ = strconv.Atoi(os.Args[1])
	}
	// alternate, so drift on the machine or the network hits both alike
	res := map[bool][][2]float64{}
	for round := 0; round < 3; round++ {
		for _, routing := range []bool{false, true} {
			m, p := measure(nodes, os.Getenv("KURWA_PASSWORD"), routing, count)
			res[routing] = append(res[routing], [2]float64{m, p})
		}
	}
	for _, routing := range []bool{false, true} {
		rs := res[routing]
		ms, ps := []float64{}, []float64{}
		for _, r := range rs {
			ms, ps = append(ms, r[0]), append(ps, r[1])
		}
		sort.Float64s(ms)
		sort.Float64s(ps)
		name := "off"
		if routing {
			name = "on"
		}
		fmt.Printf("routing %s: median %.0f µs, p99 %.0f µs (%d sequential Has, median of 3 runs)\n", name, ms[1], ps[1], count)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

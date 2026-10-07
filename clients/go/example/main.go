// An idempotent payment handler: only the first caller for a payment id
// charges, however many retries and replicas there are.
//
//	KURWA_NODES=10.10.10.112:26379 KURWA_TOKEN=... go run ./example pay-1029
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kurwadb "github.com/holooloo/kurwadb/clients/go"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: example <payment-id>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodes := strings.Split(envOr("KURWA_NODES", "127.0.0.1:6379"), ",")
	db, err := kurwadb.Connect(ctx, kurwadb.Options{Nodes: nodes, Password: os.Getenv("KURWA_TOKEN")})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, n := range db.Nodes() {
		fmt.Printf("node %s at %s:%d up=%v\n", n.Name, n.Host, n.Port, n.Up)
	}

	first, err := db.AddNew(ctx, "payment:"+os.Args[1], kurwadb.TTL(24*time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	if first {
		fmt.Println("charging", os.Args[1])
	} else {
		fmt.Println(os.Args[1], "was already charged")
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

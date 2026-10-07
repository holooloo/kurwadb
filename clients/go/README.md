# kurwadb for Go

A client for [kurwadb](../../README.md): add a key, ask whether it is there,
delete it - and `AddNew`, the atomic "first one wins" that idempotency checks
are built on. Standard library only, safe for concurrent use. The Go
counterpart of [`clients/js`](../js), with the same behaviour.

```sh
go get github.com/holooloo/kurwadb/clients/go
```

```go
db, err := kurwadb.Connect(ctx, kurwadb.Options{
	Nodes:    []string{"10.10.10.112:26379"},
	Password: os.Getenv("KURWA_TOKEN"),
})
defer db.Close()

if first, _ := db.AddNew(ctx, "payment:"+id, kurwadb.TTL(24*time.Hour)); first {
	charge() // exactly one caller gets here
}

db.Add(ctx, "order:1029")
db.Has(ctx, "order:1029")                    // true
db.HasMany(ctx, []string{"a", "b", "c"})     // [true false true], one round trip
db.Delete(ctx, "order:1029")                 // true, then false

seen := db.Set("seen")                       // a named set
seen.Add(ctx, "ip:10.0.0.1")
seen.Has(ctx, "ip:10.0.0.1")

db.Nodes()                                   // []NodeInfo{Name, Host, Port, Up, InFlight}
```

`go run ./example pay-1029` with `KURWA_NODES` and `KURWA_TOKEN` set runs the
idempotent-payment example.

## How it talks to the cluster

RESP3 to each node's Redis frontend (`HELLO 3`, the token as the password,
`kurwadb-go/<version>` as the client name, which the dashboard shows), with
requests pipelined: many in flight on a connection, a FIFO of waiters, one
reader goroutine handing each reply to the next.

**Discovery.** Any one node is enough to start. The client asks it for the
cluster's members with `KURWA.NODES` - each node's name and the address
clients reach its RESP frontend at - and asks again every 10 seconds and
whenever a node fails.

**Routing.** Every kurwadb node coordinates any request, so each request
goes to the healthy node with the fewest requests in flight, over the least
busy of its connections (two per node by default).

**Failover.** A connection that fails marks its node down; its in-flight
requests fail with `CodeConnection`. `Add`, `Has` and `HasMany` are retried
once on another node, since running them twice is harmless. `AddNew` and
`Delete` are not: their answer says what *this* call changed, and a retry
after a first attempt that did land would answer wrongly. A node marked down
is probed every 2 seconds and used again once it answers.

## Options

| field | default | |
|---|---|---|
| `Nodes` | `127.0.0.1:6379` | seeds, `host:port` of RESP frontends |
| `Password` | none | kurwadb's auth token |
| `User` | `kurwa` | any name; kurwadb has one secret |
| `PoolSize` | 2 | connections per node |
| `ConnectTimeout` | 3s | dialling and HELLO |
| `Timeout` | 5s | per request; a request that times out may still have run |
| `RefreshInterval` | 10s | between `KURWA.NODES`; negative never |
| `ProbeInterval` | 2s | between retries of a down node |
| `NoDiscover` | false | true uses exactly `Nodes` |
| `Name` | `kurwadb-go/<version>` | the client name the server sees |

Every method takes a `context.Context`; its deadline or cancellation ends the
wait.

## Errors

Everything returns `*kurwadb.Error`, whose `Code` is `SERVER` (kurwadb said
no, with its message), `CONNECTION`, `TIMEOUT`, `UNSUPPORTED` or `CLOSED`;
`kurwadb.IsCode(err, kurwadb.CodeTimeout)` tests for one.

## Limits

- Over RESP, named-set keys take no TTL and have no atomic `AddNew`: `SADD`
  looks, then writes. Both work on the default set (`SET ... PX`, `SET NX`).
- Routing does not follow the ring: a request costs one hop from the node it
  lands on to the replicas, whichever node that is.
- No TLS yet.

## Tests

```sh
go test -race ./...    # starts a kurwadb node from this repository (needs elixir)
KURWA_RESP_NODES=10.10.10.112:26379 KURWA_PASSWORD=... go test -race ./...   # against a running one
```

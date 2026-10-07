// Package kurwadb is a client for kurwadb, a distributed set of keys.
//
//	db, err := kurwadb.Connect(ctx, kurwadb.Options{
//		Nodes:    []string{"10.10.10.112:26379"},
//		Password: os.Getenv("KURWA_TOKEN"),
//	})
//	first, err := db.AddNew(ctx, "payment:"+id, kurwadb.TTL(24*time.Hour))
//
// Any kurwadb node coordinates any request, so the client needs no routing
// table: it learns the cluster's nodes from one of them (KURWA.NODES), keeps
// a few pipelined connections to each, sends every request to the healthy
// node with the fewest in flight, and moves to another node when one fails.
package kurwadb

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Version is this client's version; the dashboard shows it as kurwadb-go/<Version>.
const Version = "0.1.0"

// Options configure Connect. The zero value of each field means its default.
type Options struct {
	// Nodes are seeds, "host:port" of RESP frontends. Any one is enough.
	Nodes []string
	// Password is kurwadb's auth token.
	Password string
	// User is any name; kurwadb has one secret. Default "kurwa".
	User string
	// PoolSize is connections per node. Default 2.
	PoolSize int
	// ConnectTimeout bounds dialling and HELLO. Default 3s.
	ConnectTimeout time.Duration
	// Timeout bounds a request. Default 5s. A request that times out may
	// still have run.
	Timeout time.Duration
	// RefreshInterval is how often to ask for the cluster's nodes. Default
	// 10s; negative never.
	RefreshInterval time.Duration
	// ProbeInterval is how often a down node is tried again. Default 2s.
	ProbeInterval time.Duration
	// NoDiscover uses exactly Nodes, without asking the cluster for more.
	NoDiscover bool
	// Name is the client name the server and its dashboard see.
	// Default "kurwadb-go/<Version>".
	Name string
}

func (o Options) withDefaults() Options {
	if len(o.Nodes) == 0 {
		o.Nodes = []string{"127.0.0.1:6379"}
	}
	if o.User == "" {
		o.User = "kurwa"
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 2
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 3 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.RefreshInterval == 0 {
		o.RefreshInterval = 10 * time.Second
	}
	if o.ProbeInterval <= 0 {
		o.ProbeInterval = 2 * time.Second
	}
	if o.Name == "" {
		o.Name = "kurwadb-go/" + Version
	}
	return o
}

// NodeInfo is what the client knows about one node.
type NodeInfo struct {
	Name     string
	Host     string
	Port     int
	Up       bool
	InFlight int
}

// ---------------------------------------------------------------- nodes

type node struct {
	client *Client
	host   string
	port   int

	mu     sync.Mutex
	name   string
	up     bool
	conns  []*conn
	gen    int // bumped by markDown, so a pool opened before it is not kept
	openMu sync.Mutex
}

func (n *node) key() string { return net.JoinHostPort(n.host, strconv.Itoa(n.port)) }

func (n *node) info() NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeInfo{Name: n.name, Host: n.host, Port: n.port, Up: n.up, InFlight: n.loadLocked()}
}

func (n *node) isUp() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.up
}

func (n *node) load() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.loadLocked()
}

func (n *node) loadLocked() int {
	sum := 0
	for _, c := range n.conns {
		sum += c.load()
	}
	return sum
}

func (n *node) live() []*conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	kept := n.conns[:0]
	for _, c := range n.conns {
		if !c.isClosed() {
			kept = append(kept, c)
		}
	}
	n.conns = kept
	return append([]*conn(nil), kept...)
}

// connection returns the least busy open connection, opening the pool on
// first use and topping it up in the background after that.
func (n *node) connection(ctx context.Context) (*conn, error) {
	conns := n.live()
	if len(conns) == 0 {
		if err := n.open(ctx); err != nil {
			return nil, err
		}
		conns = n.live()
	} else if len(conns) < n.client.opts.PoolSize {
		go func() { _ = n.open(context.Background()) }()
	}
	if len(conns) == 0 {
		return nil, errorf(CodeConnection, n.key(), "kurwadb: no connection to %s", n.key())
	}
	best := conns[0]
	for _, c := range conns[1:] {
		if c.load() < best.load() {
			best = c
		}
	}
	return best, nil
}

// open adds one connection, unless the pool filled up meanwhile.
func (n *node) open(ctx context.Context) error {
	n.openMu.Lock()
	defer n.openMu.Unlock()
	if len(n.live()) >= n.client.opts.PoolSize {
		return nil
	}
	n.mu.Lock()
	gen, label := n.gen, n.name
	n.mu.Unlock()
	c, err := dial(ctx, n.host, n.port, &n.client.opts, label)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if gen != n.gen || n.client.isClosed() {
		c.close()
		return errorf(CodeConnection, label, "kurwadb: %s went down while connecting", label)
	}
	n.conns = append(n.conns, c)
	return nil
}

func (n *node) markDown() {
	n.mu.Lock()
	n.up = false
	n.gen++
	conns := n.conns
	n.conns = nil
	n.mu.Unlock()
	for _, c := range conns {
		c.close()
	}
}

func (n *node) markUp() {
	n.mu.Lock()
	n.up = true
	n.mu.Unlock()
}

func (n *node) closeAll() {
	n.mu.Lock()
	conns := n.conns
	n.conns = nil
	n.gen++
	n.mu.Unlock()
	for _, c := range conns {
		c.close()
	}
}

// --------------------------------------------------------------- client

// Client is a connection to a kurwadb cluster. It is safe for concurrent use.
type Client struct {
	opts Options

	mu     sync.RWMutex
	nodes  map[string]*node
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

// Connect reaches the cluster through the first seed that answers, learns
// its other nodes unless NoDiscover, and starts refreshing and probing them.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	c := &Client{opts: opts.withDefaults(), nodes: map[string]*node{}, done: make(chan struct{})}

	var lastErr error
	for _, seed := range c.opts.Nodes {
		host, port, err := parseAddress(seed)
		if err != nil {
			lastErr = err
			continue
		}
		n := c.newNode(host, port, "")
		if _, err := n.connection(ctx); err != nil {
			n.closeAll()
			lastErr = err
			continue
		}
		c.mu.Lock()
		c.nodes[n.key()] = n
		c.mu.Unlock()
		if !c.opts.NoDiscover {
			if err := c.refresh(ctx, n); err != nil {
				lastErr = err
			}
		}
		break
	}
	c.mu.RLock()
	empty := len(c.nodes) == 0
	c.mu.RUnlock()
	if empty {
		if lastErr == nil {
			lastErr = errorf(CodeConnection, "", "kurwadb: no seed node answered")
		}
		return nil, lastErr
	}
	if c.opts.NoDiscover {
		for _, seed := range c.opts.Nodes {
			if host, port, err := parseAddress(seed); err == nil {
				c.mu.Lock()
				if _, ok := c.nodes[net.JoinHostPort(host, strconv.Itoa(port))]; !ok {
					n := c.newNode(host, port, "")
					c.nodes[n.key()] = n
				}
				c.mu.Unlock()
			}
		}
	}
	c.wg.Add(1)
	go c.background()
	return c, nil
}

func (c *Client) newNode(host string, port int, name string) *node {
	n := &node{client: c, host: host, port: port, up: true}
	n.name = name
	if n.name == "" {
		n.name = n.key()
	}
	return n
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Client) background() {
	defer c.wg.Done()
	probe := time.NewTicker(c.opts.ProbeInterval)
	defer probe.Stop()
	var refresh <-chan time.Time
	if !c.opts.NoDiscover && c.opts.RefreshInterval > 0 {
		t := time.NewTicker(c.opts.RefreshInterval)
		defer t.Stop()
		refresh = t.C
	}
	for {
		select {
		case <-c.done:
			return
		case <-probe.C:
			c.probe()
		case <-refresh:
			ctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
			_ = c.refresh(ctx, nil)
			cancel()
		}
	}
}

// refresh asks a node for the cluster's members and their addresses. Nodes
// that left are dropped, new ones added; a node the cluster says is down is
// not sent requests until a probe reaches it.
func (c *Client) refresh(ctx context.Context, via *node) error {
	if via == nil {
		var err error
		if via, err = c.pick(nil); err != nil {
			return err
		}
	}
	conn, err := via.connection(ctx)
	if err != nil {
		return err
	}
	replies, err := conn.send(ctx, [][]string{{"KURWA.NODES"}}, c.opts.Timeout)
	if err != nil {
		if IsCode(err, CodeServer) {
			return nil // a server without KURWA.NODES: keep the seeds
		}
		return err
	}
	entries, _ := replies[0].([]any)
	seen := map[string]bool{}
	for _, e := range entries {
		f, ok := e.([]any)
		if !ok || len(f) < 4 {
			continue
		}
		name, host := fmt.Sprint(f[0]), fmt.Sprint(f[1])
		port, err := toInt(f[2])
		if err != nil {
			continue
		}
		key := net.JoinHostPort(host, strconv.Itoa(port))
		seen[key] = true
		c.mu.Lock()
		known, ok := c.nodes[key]
		if !ok {
			known = c.newNode(host, port, name)
			c.nodes[key] = known
		}
		c.mu.Unlock()
		known.mu.Lock()
		known.name = name
		known.mu.Unlock()
		if !truthy(f[3]) && known.isUp() {
			known.markDown()
		}
	}
	if len(seen) == 0 {
		return nil
	}
	// the node asked stays even if it reports itself by another address
	c.mu.Lock()
	var gone []*node
	for key, n := range c.nodes {
		if !seen[key] && n != via {
			gone = append(gone, n)
			delete(c.nodes, key)
		}
	}
	c.mu.Unlock()
	for _, n := range gone {
		n.closeAll()
	}
	return nil
}

func (c *Client) probe() {
	for _, n := range c.snapshot() {
		if n.isUp() || c.isClosed() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.ConnectTimeout)
		conn, err := n.connection(ctx)
		if err == nil {
			_, err = conn.send(ctx, [][]string{{"PING"}}, c.opts.ConnectTimeout)
		}
		cancel()
		if err == nil {
			n.markUp()
		} else {
			n.markDown()
		}
	}
}

func (c *Client) snapshot() []*node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n)
	}
	return out
}

func (c *Client) pick(exclude *node) (*node, error) {
	var best *node
	bestLoad := math.MaxInt
	for _, n := range c.snapshot() {
		if n == exclude || !n.isUp() {
			continue
		}
		if l := n.load(); l < bestLoad {
			best, bestLoad = n, l
		}
	}
	if best == nil {
		return nil, errorf(CodeConnection, "", "kurwadb: no node is reachable")
	}
	return best, nil
}

// run sends commands to one node. A connection failure marks the node down;
// an idempotent request is then tried once more on another node.
func (c *Client) run(ctx context.Context, commands [][]string, idempotent bool) ([]any, error) {
	if c.isClosed() {
		return nil, errorf(CodeClosed, "", "kurwadb: client is closed")
	}
	n, err := c.pick(nil)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		var replies []any
		conn, err := n.connection(ctx)
		if err == nil {
			replies, err = conn.send(ctx, commands, c.opts.Timeout)
			if err == nil {
				return replies, nil
			}
		}
		if !IsCode(err, CodeConnection) || ctx.Err() != nil {
			return nil, err
		}
		n.markDown()
		if attempt > 0 || !idempotent {
			return nil, err
		}
		if !c.opts.NoDiscover {
			go func() {
				rctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
				defer cancel()
				_ = c.refresh(rctx, nil)
			}()
		}
		if n, err = c.pick(n); err != nil {
			return nil, err
		}
	}
}

// ------------------------------------------------------------------ API

// Option modifies a write.
type Option func(*writeOptions)

type writeOptions struct{ ttl time.Duration }

// TTL makes the key expire after d (rounded to milliseconds, at least 1ms).
func TTL(d time.Duration) Option { return func(o *writeOptions) { o.ttl = d } }

func px(opts []Option) ([]string, bool) {
	var w writeOptions
	for _, o := range opts {
		o(&w)
	}
	if w.ttl <= 0 {
		return nil, false
	}
	ms := w.ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return []string{"PX", strconv.FormatInt(ms, 10)}, true
}

// Add adds key to the default set.
func (c *Client) Add(ctx context.Context, key string, opts ...Option) error {
	ttl, _ := px(opts)
	_, err := c.run(ctx, [][]string{append([]string{"SET", key, "1"}, ttl...)}, true)
	return err
}

// AddNew adds key only if it is not there: true for the one caller that
// added it, false for everyone else - the idempotency check. It is not
// retried on another node, since a retry could be told "exists" by its own
// first attempt.
func (c *Client) AddNew(ctx context.Context, key string, opts ...Option) (bool, error) {
	ttl, _ := px(opts)
	r, err := c.run(ctx, [][]string{append([]string{"SET", key, "1", "NX"}, ttl...)}, false)
	if err != nil {
		return false, err
	}
	return r[0] != nil, nil
}

// Has reports whether key is in the default set.
func (c *Client) Has(ctx context.Context, key string) (bool, error) {
	r, err := c.run(ctx, [][]string{{"EXISTS", key}}, true)
	if err != nil {
		return false, err
	}
	return truthy(r[0]), nil
}

// HasMany reports the membership of each key, in order, in one round trip.
func (c *Client) HasMany(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return []bool{}, nil
	}
	cmds := make([][]string, len(keys))
	for i, k := range keys {
		cmds[i] = []string{"EXISTS", k}
	}
	r, err := c.run(ctx, cmds, true)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(r))
	for i, v := range r {
		out[i] = truthy(v)
	}
	return out, nil
}

// Delete removes key. True if it was there.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	r, err := c.run(ctx, [][]string{{"DEL", key}}, false)
	if err != nil {
		return false, err
	}
	return truthy(r[0]), nil
}

// Set is the same operations on the named set name.
func (c *Client) Set(name string) *Set { return &Set{c: c, name: name} }

// Nodes are the nodes this client knows, with their state and requests in flight.
func (c *Client) Nodes() []NodeInfo {
	nodes := c.snapshot()
	out := make([]NodeInfo, len(nodes))
	for i, n := range nodes {
		out[i] = n.info()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Close closes every connection and stops the background work.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	c.wg.Wait()
	for _, n := range c.snapshot() {
		n.closeAll()
	}
	return nil
}

// Set is a named set.
type Set struct {
	c    *Client
	name string
}

// Add adds key. Named sets take no TTL over RESP (SADD has none).
func (s *Set) Add(ctx context.Context, key string, opts ...Option) error {
	if _, ttl := px(opts); ttl {
		return errorf(CodeUnsupported, "", "kurwadb: a TTL on a named-set key is not available over RESP; use the default set")
	}
	_, err := s.c.run(ctx, [][]string{{"SADD", s.name, key}}, true)
	return err
}

// AddNew is not available for named sets over RESP: SADD looks, then writes.
func (s *Set) AddNew(ctx context.Context, key string, opts ...Option) (bool, error) {
	return false, errorf(CodeUnsupported, "", "kurwadb: AddNew is only atomic on the default set over RESP (SET NX); use Client.AddNew")
}

// Has reports whether key is in the set.
func (s *Set) Has(ctx context.Context, key string) (bool, error) {
	r, err := s.c.run(ctx, [][]string{{"SISMEMBER", s.name, key}}, true)
	if err != nil {
		return false, err
	}
	return truthy(r[0]), nil
}

// HasMany reports the membership of each key, in order.
func (s *Set) HasMany(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return []bool{}, nil
	}
	r, err := s.c.run(ctx, [][]string{append([]string{"SMISMEMBER", s.name}, keys...)}, true)
	if err != nil {
		return nil, err
	}
	items, _ := r[0].([]any)
	out := make([]bool, len(items))
	for i, v := range items {
		out[i] = truthy(v)
	}
	return out, nil
}

// Delete removes key. True if it was there.
func (s *Set) Delete(ctx context.Context, key string) (bool, error) {
	r, err := s.c.run(ctx, [][]string{{"SREM", s.name, key}}, false)
	if err != nil {
		return false, err
	}
	return truthy(r[0]), nil
}

// --------------------------------------------------------------- helpers

func parseAddress(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return address, 6379, nil
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, errorf(CodeConnection, address, "kurwadb: bad address %q", address)
	}
	return host, port, nil
}

func toInt(v any) (int, error) {
	switch x := v.(type) {
	case int64:
		return int(x), nil
	case string:
		return strconv.Atoi(x)
	}
	return 0, fmt.Errorf("not a number: %v", v)
}

package kurwadb

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The tests run against KURWA_RESP_NODES (comma-separated host:port) with
// KURWA_PASSWORD when given, else against a node started here from this
// repository (needs elixir).
var (
	seeds    []string
	password string
	db       *Client
)

func TestMain(m *testing.M) {
	stop, err := startServer()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	db, err = Connect(ctx, Options{Nodes: seeds, Password: password})
	cancel()
	if err != nil {
		stop()
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = db.Close()
	stop()
	os.Exit(code)
}

func startServer() (func(), error) {
	if env := os.Getenv("KURWA_RESP_NODES"); env != "" {
		seeds = strings.Split(env, ",")
		password = os.Getenv("KURWA_PASSWORD")
		return func() {}, nil
	}
	port, http := freePort(), freePort()
	password = "go-test-secret"
	dataDir, err := os.MkdirTemp("", "kurwadb-go-")
	if err != nil {
		return nil, err
	}
	repo, _ := filepath.Abs("../..")
	home, _ := os.UserHomeDir()
	cmd := exec.Command("elixir", "-S", "mix", "run", "--no-halt")
	cmd.Dir = repo
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(),
		"PATH="+home+"/.cargo/bin:/opt/homebrew/bin:"+os.Getenv("PATH"),
		"KURWA_RESP=1", "KURWA_RESP_PORT="+strconv.Itoa(port),
		"KURWA_HTTP_PORT="+strconv.Itoa(http),
		"KURWA_N=1", "KURWA_R=1", "KURWA_W=1",
		"KURWA_DATA_DIR="+dataDir, "KURWA_AUTH_TOKEN="+password)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stop := func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		_ = os.RemoveAll(dataDir)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) || cmd.ProcessState != nil {
			stop()
			return nil, fmt.Errorf("kurwadb did not start:\n%s", stderr.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
	seeds = []string{addr}
	return stop, nil
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func id() string { return fmt.Sprintf("go:%d:%x", os.Getpid(), rand.Int63()) }

// mb and mbs unwrap a (value, error) pair; an error panics, failing the test.
func mb(v bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return v
}

func mbs(v []bool, err error) []bool {
	if err != nil {
		panic(err)
	}
	return v
}

func TestAddHasDelete(t *testing.T) {
	ctx := context.Background()
	k := id()
	if mb(db.Has(ctx, k)) {
		t.Fatal("present before add")
	}
	if err := db.Add(ctx, k); err != nil {
		t.Fatal(err)
	}
	if !mb(db.Has(ctx, k)) {
		t.Fatal("absent after add")
	}
	if !mb(db.Delete(ctx, k)) {
		t.Fatal("delete said absent")
	}
	if mb(db.Delete(ctx, k)) {
		t.Fatal("second delete said present")
	}
	if mb(db.Has(ctx, k)) {
		t.Fatal("present after delete")
	}
}

func TestAddNewHasOneWinner(t *testing.T) {
	ctx := context.Background()
	k := id()
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, err := db.AddNew(ctx, k, TTL(time.Minute))
			if err != nil {
				t.Error(err)
				return
			}
			if won {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d winners, want 1", wins)
	}
	if mb(db.AddNew(ctx, k)) {
		t.Fatal("AddNew after the winner won")
	}
}

func TestTTLExpires(t *testing.T) {
	ctx := context.Background()
	k := id()
	if err := db.Add(ctx, k, TTL(300*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !mb(db.Has(ctx, k)) {
		t.Fatal("absent right after add")
	}
	time.Sleep(700 * time.Millisecond)
	if mb(db.Has(ctx, k)) {
		t.Fatal("present after its ttl")
	}
}

func TestHasManyInOrder(t *testing.T) {
	ctx := context.Background()
	a, b, c := id(), id(), id()
	_ = db.Add(ctx, a)
	_ = db.Add(ctx, c)
	got := mbs(db.HasMany(ctx, []string{a, b, c}))
	if fmt.Sprint(got) != "[true false true]" {
		t.Fatalf("got %v", got)
	}
	if got := mbs(db.HasMany(ctx, nil)); len(got) != 0 {
		t.Fatalf("got %v for no keys", got)
	}
}

func TestBinaryKeys(t *testing.T) {
	ctx := context.Background()
	k := string([]byte{0, 255, 1, 254, 10, 13})
	if err := db.Add(ctx, k); err != nil {
		t.Fatal(err)
	}
	if !mb(db.Has(ctx, k)) {
		t.Fatal("binary key absent")
	}
	if mb(db.Has(ctx, string([]byte{0, 255, 1}))) {
		t.Fatal("prefix of a binary key present")
	}
}

func TestNamedSets(t *testing.T) {
	ctx := context.Background()
	s := db.Set(fmt.Sprintf("goset-%d", os.Getpid()))
	k := id()
	if err := s.Add(ctx, k); err != nil {
		t.Fatal(err)
	}
	if !mb(s.Has(ctx, k)) {
		t.Fatal("absent from the named set")
	}
	if mb(db.Has(ctx, k)) {
		t.Fatal("a named set is not the default set")
	}
	if got := mbs(s.HasMany(ctx, []string{k, id()})); fmt.Sprint(got) != "[true false]" {
		t.Fatalf("got %v", got)
	}
	if !mb(s.Delete(ctx, k)) || mb(s.Has(ctx, k)) {
		t.Fatal("delete from the named set")
	}
	if err := s.Add(ctx, k, TTL(5*time.Second)); !IsCode(err, CodeUnsupported) {
		t.Fatalf("ttl on a named set: %v", err)
	}
	if _, err := s.AddNew(ctx, k); !IsCode(err, CodeUnsupported) {
		t.Fatalf("AddNew on a named set: %v", err)
	}
}

func TestThousandsPipelined(t *testing.T) {
	ctx := context.Background()
	keys := make([]string, 2000)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s:%d", id(), i)
	}
	var wg sync.WaitGroup
	for i := 0; i < len(keys); i += 2 {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			if err := db.Add(ctx, k); err != nil {
				t.Error(err)
			}
		}(keys[i])
	}
	wg.Wait()
	answers := make([]bool, len(keys))
	for i, k := range keys {
		wg.Add(1)
		go func(i int, k string) {
			defer wg.Done()
			v, err := db.Has(ctx, k)
			if err != nil {
				t.Error(err)
			}
			answers[i] = v
		}(i, k)
	}
	wg.Wait()
	for i, v := range answers {
		if v != (i%2 == 0) {
			t.Fatalf("key %d: %v", i, v)
		}
	}
}

func TestDiscoversTheCluster(t *testing.T) {
	nodes := db.Nodes()
	if len(nodes) == 0 {
		t.Fatal("no nodes")
	}
	up := false
	for _, n := range nodes {
		if n.Name == "" || n.Port <= 0 {
			t.Fatalf("bad node %+v", n)
		}
		up = up || n.Up
	}
	if !up {
		t.Fatal("no node up")
	}
	t.Logf("nodes: %+v", nodes)
}

func TestServerErrors(t *testing.T) {
	ctx := context.Background()
	_, err := Connect(ctx, Options{Nodes: seeds, Password: "wrong", NoDiscover: true})
	e, ok := err.(*Error)
	if !ok || e.Code != CodeServer || !strings.Contains(strings.ToUpper(e.Message), "WRONGPASS") && !strings.Contains(strings.ToLower(e.Message), "invalid") {
		t.Fatalf("got %v", err)
	}
}

func TestDeadSeedSkipped(t *testing.T) {
	ctx := context.Background()
	c, err := Connect(ctx, Options{Nodes: append([]string{"127.0.0.1:1"}, seeds...), Password: password, ConnectTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if mb(c.Has(ctx, id())) {
		t.Fatal("random key present")
	}
}

func TestClosedClient(t *testing.T) {
	c, err := Connect(context.Background(), Options{Nodes: seeds, Password: password, NoDiscover: true})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if _, err := c.Has(context.Background(), "x"); !IsCode(err, CodeClosed) {
		t.Fatalf("got %v", err)
	}
}

// proxy forwards a local port to target ("host:port") until killed: a node
// that can disappear from under a client.
type proxy struct {
	l     net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func startProxy(t *testing.T, target string, listen string) *proxy {
	t.Helper()
	l, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{l: l}
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go func() { io.Copy(upstream, client); upstream.Close(); client.Close() }()
			go func() { io.Copy(client, upstream); upstream.Close(); client.Close() }()
		}
	}()
	return p
}

func (p *proxy) addr() string { return p.l.Addr().String() }

func (p *proxy) kill() {
	p.l.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

func TestFailover(t *testing.T) {
	ctx := context.Background()
	p := startProxy(t, seeds[0], "127.0.0.1:0")
	addr := p.addr()
	c, err := Connect(ctx, Options{Nodes: append([]string{addr}, seeds...), Password: password, NoDiscover: true, ProbeInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	k := id()
	if err := c.Add(ctx, k); err != nil {
		t.Fatal(err)
	}

	// load both nodes, then kill one mid-flight
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Has(ctx, k)
			if err == nil && !v {
				err = fmt.Errorf("absent")
			}
			if err != nil {
				errs <- err
			}
		}()
	}
	p.kill()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("an idempotent read was not retried elsewhere: %v", err)
	}
	for i := 0; i < 50; i++ {
		if !mb(c.Has(ctx, k)) {
			t.Fatal("absent after failover")
		}
	}
	if nodeUp(c, addr) {
		t.Fatal("the killed node is still up")
	}

	// the node comes back on the same address: a probe finds it
	again := startProxy(t, seeds[0], addr)
	defer again.kill()
	deadline := time.Now().Add(3 * time.Second)
	for !nodeUp(c, addr) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !nodeUp(c, addr) {
		t.Fatal("the node came back but was not probed up")
	}
}

func nodeUp(c *Client, addr string) bool {
	for _, n := range c.Nodes() {
		if net.JoinHostPort(n.Host, strconv.Itoa(n.Port)) == addr {
			return n.Up
		}
	}
	return false
}

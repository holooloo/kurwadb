package kurwadb

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"sync"
	"time"
)

// conn is one TCP connection to one node: HELLO 3 with AUTH and a client
// name, then pipelined commands. Writers append to a FIFO of waiters and
// one reader goroutine hands each reply to the next waiter, so replies stay
// in step with requests even when a caller stopped waiting.
type conn struct {
	nc    net.Conn
	label string

	wmu sync.Mutex // one writer at a time: enqueue order is write order
	buf []byte

	mu      sync.Mutex
	pending []chan result
	closed  bool
	err     *Error
}

type result struct {
	v   any
	err error
}

func dial(ctx context.Context, host string, port int, o *Options, label string) (*conn, error) {
	d := net.Dialer{Timeout: o.ConnectTimeout}
	nc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, errorf(CodeConnection, label, "kurwadb: %s: %v", label, err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c := &conn{nc: nc, label: label}
	go c.read()

	hello := []string{"HELLO", "3"}
	if o.Password != "" {
		hello = append(hello, "AUTH", o.User, o.Password)
	}
	if o.Name != "" {
		hello = append(hello, "SETNAME", o.Name)
	}
	hctx, cancel := context.WithTimeout(ctx, o.ConnectTimeout)
	defer cancel()
	if _, err := c.send(hctx, [][]string{hello}, o.ConnectTimeout); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}

// load is the number of requests sent and not answered yet.
func (c *conn) load() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

func (c *conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// send writes every command in one write and waits for all their replies.
// A server error in any of them is the error of the whole call.
func (c *conn) send(ctx context.Context, commands [][]string, timeout time.Duration) ([]any, error) {
	waiters := make([]chan result, len(commands))
	for i := range waiters {
		waiters[i] = make(chan result, 1)
	}

	c.wmu.Lock()
	c.mu.Lock()
	if c.closed {
		err := c.err
		c.mu.Unlock()
		c.wmu.Unlock()
		return nil, err
	}
	c.pending = append(c.pending, waiters...)
	c.mu.Unlock()
	c.buf = c.buf[:0]
	for _, cmd := range commands {
		c.buf = appendCommand(c.buf, cmd)
	}
	_, werr := c.nc.Write(c.buf)
	c.wmu.Unlock()
	if werr != nil {
		c.fail(errorf(CodeConnection, c.label, "kurwadb: %s: %v", c.label, werr))
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	replies := make([]any, len(commands))
	var first error
	for i, w := range waiters {
		select {
		case r := <-w:
			if r.err != nil && first == nil {
				first = r.err
			}
			replies[i] = r.v
		case <-timer.C:
			// The replies may still come; their waiters stay queued and are
			// drained by the reader, so later replies keep their places.
			return nil, errorf(CodeTimeout, c.label, "kurwadb: %s did not answer within %v", c.label, timeout)
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil, errorf(CodeTimeout, c.label, "kurwadb: %s: %v", c.label, ctx.Err())
			}
			return nil, &Error{Code: CodeConnection, Node: c.label, Message: "kurwadb: " + ctx.Err().Error()}
		}
	}
	return replies, first
}

func (c *conn) read() {
	r := bufio.NewReaderSize(c.nc, 64*1024)
	for {
		v, err := readReply(r)
		if err != nil {
			c.fail(errorf(CodeConnection, c.label, "kurwadb: connection to %s closed: %v", c.label, err))
			return
		}
		c.mu.Lock()
		if len(c.pending) == 0 {
			c.mu.Unlock()
			c.fail(errorf(CodeConnection, c.label, "kurwadb: %s sent a reply nobody asked for", c.label))
			return
		}
		w := c.pending[0]
		c.pending[0] = nil
		c.pending = c.pending[1:]
		c.mu.Unlock()
		if e, ok := v.(replyError); ok {
			w <- result{err: &Error{Code: CodeServer, Node: c.label, Message: e.msg}}
		} else {
			w <- result{v: v}
		}
	}
}

// fail closes the connection and fails every request still waiting on it.
func (c *conn) fail(err *Error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.err = err
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	_ = c.nc.Close()
	for _, w := range pending {
		w <- result{err: err}
	}
}

func (c *conn) close() {
	c.fail(errorf(CodeConnection, c.label, "kurwadb: connection to %s closed", c.label))
}

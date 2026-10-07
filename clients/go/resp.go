package kurwadb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Commands go out as RESP arrays of bulk strings, whatever the protocol
// version: every server reads those.
func appendCommand(buf []byte, args []string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// replyError is an error reply (-ERR, !bulk error) inside the stream; it
// becomes an *Error with CodeServer for the request it answers.
type replyError struct{ msg string }

func (e replyError) Error() string { return e.msg }

var errProtocol = errors.New("kurwadb: malformed reply")

// readReply reads one RESP2 or RESP3 reply. Bulk strings come back as
// string, integers as int64, null as nil, booleans as bool, doubles as
// float64, arrays and sets as []any, maps as map[string]any, and errors as
// replyError. Push messages (>) are skipped.
func readReply(r *bufio.Reader) (any, error) {
	for {
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			return nil, errProtocol
		}
		kind, rest := line[0], string(line[1:])
		switch kind {
		case '+':
			return rest, nil
		case '-':
			return replyError{rest}, nil
		case ':':
			return strconv.ParseInt(rest, 10, 64)
		case '(':
			return rest, nil
		case ',':
			return strconv.ParseFloat(rest, 64)
		case '#':
			return rest == "t", nil
		case '_':
			return nil, nil
		case '$', '=', '!':
			n, err := strconv.Atoi(rest)
			if err != nil {
				return nil, errProtocol
			}
			if n < 0 {
				return nil, nil
			}
			data := make([]byte, n+2)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, err
			}
			s := string(data[:n])
			if kind == '!' {
				return replyError{s}, nil
			}
			if kind == '=' && len(s) >= 4 { // verbatim: "txt:" prefix
				s = s[4:]
			}
			return s, nil
		case '*', '~', '>':
			n, err := strconv.Atoi(rest)
			if err != nil {
				return nil, errProtocol
			}
			if n < 0 {
				return nil, nil
			}
			items := make([]any, n)
			for i := range items {
				if items[i], err = readReply(r); err != nil {
					return nil, err
				}
			}
			if kind == '>' {
				continue // a push, not an answer to anything we sent
			}
			return items, nil
		case '%', '|':
			n, err := strconv.Atoi(rest)
			if err != nil {
				return nil, errProtocol
			}
			m := make(map[string]any, n)
			for i := 0; i < n; i++ {
				k, err := readReply(r)
				if err != nil {
					return nil, err
				}
				v, err := readReply(r)
				if err != nil {
					return nil, err
				}
				m[fmt.Sprint(k)] = v
			}
			if kind == '|' {
				continue // attributes precede the real reply
			}
			return m, nil
		default:
			return nil, errProtocol
		}
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		var long []byte
		long = append(long, line...)
		for err == bufio.ErrBufferFull {
			line, err = r.ReadSlice('\n')
			long = append(long, line...)
		}
		line = long
	}
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errProtocol
	}
	return line[:len(line)-2], nil
}

func truthy(v any) bool {
	switch x := v.(type) {
	case int64:
		return x > 0
	case bool:
		return x
	case string:
		return x == "1"
	}
	return false
}

package kurwadb

import "fmt"

// Code says what kind of failure an Error is.
type Code string

const (
	// CodeServer: kurwadb answered with an error; Message is its text.
	CodeServer Code = "SERVER"
	// CodeConnection: the node could not be reached, or the connection broke.
	CodeConnection Code = "CONNECTION"
	// CodeTimeout: no answer in time. The request may still have run.
	CodeTimeout Code = "TIMEOUT"
	// CodeUnsupported: the operation is not available over RESP.
	CodeUnsupported Code = "UNSUPPORTED"
	// CodeClosed: the client was closed.
	CodeClosed Code = "CLOSED"
)

// Error is every error this package returns.
type Error struct {
	Code    Code
	Message string
	// Node is the node the error came from, when there is one.
	Node string
}

func (e *Error) Error() string { return e.Message }

func errorf(code Code, node, format string, args ...any) *Error {
	return &Error{Code: code, Node: node, Message: fmt.Sprintf(format, args...)}
}

// IsCode reports whether err is an *Error with the given code.
func IsCode(err error, code Code) bool {
	e, ok := err.(*Error)
	return ok && e.Code == code
}

package protocol

import "fmt"

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

func (e *Error) Error() string { return e.Message }
func E(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Details: map[string]any{}}
}

type Response struct {
	OK           bool   `json:"ok"`
	Workspace    any    `json:"workspace,omitempty"`
	Workspaces   any    `json:"workspaces,omitempty"`
	Process      any    `json:"process,omitempty"`
	Processes    any    `json:"processes,omitempty"`
	Groups       any    `json:"groups,omitempty"`
	Environments any    `json:"environments,omitempty"`
	Error        *Error `json:"error,omitempty"`
}

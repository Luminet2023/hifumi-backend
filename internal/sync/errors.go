package sync

import (
	"errors"
	"fmt"
)

var ErrResponseTooLarge = errors.New("sync response is too large")

type ErrorCode string

const (
	CodeInvalidArgument    ErrorCode = "INVALID_ARGUMENT"
	CodeFailedPrecondition ErrorCode = "FAILED_PRECONDITION"
)

type OperationError struct {
	Code    ErrorCode
	Message string
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func invalidArgument(message string) error {
	return &OperationError{Code: CodeInvalidArgument, Message: message}
}

func failedPrecondition(message string) error {
	return &OperationError{Code: CodeFailedPrecondition, Message: message}
}

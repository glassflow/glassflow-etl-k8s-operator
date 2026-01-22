package errs

import (
	"errors"
)

var (
	ErrPipelineNotFound                = errors.New("no pipeline was found")
	ErrPipelineAlreadyInTransition     = errors.New("pipeline already in transition")
	ErrPipelineInvalidStatusTransition = errors.New("invalid status transition")
	ErrUnknownStatus                   = errors.New("unknown status")
)

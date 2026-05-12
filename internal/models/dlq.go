package models

import (
	"encoding/json"
	"fmt"
)

// DLQ envelope reasons. Values are part of the wire contract — do not rename without
// coordinating with glassflow-api consumers.
const (
	DLQReasonRetryExhaustedOnStop = "retry_exhausted_on_stop"
)

// DLQSubjectSuffix is the per-stream NATS subject suffix used for DLQ messages —
// aligned with glassflow-api's internal.DLQSubjectName.
const DLQSubjectSuffix = "failed"

// DLQSubjectFromStream returns the NATS subject to publish DLQ messages on for a
// given DLQ stream (e.g. "gfm-<hash>-DLQ" → "gfm-<hash>-DLQ.failed").
func DLQSubjectFromStream(dlqStreamName string) string {
	return dlqStreamName + "." + DLQSubjectSuffix
}

// DLQMessage is the JSON envelope written to a pipeline's DLQ stream.
//
// The first three fields (Component, Error, OriginalMessage) match the legacy schema
// produced by glassflow-api components. Reason is additive: existing consumers ignore
// unknown fields, so it's safe to introduce ahead of consumer-side awareness.
type DLQMessage struct {
	Component       string `json:"component"`
	Error           string `json:"error,omitempty"`
	OriginalMessage string `json:"original_message"`
	Reason          string `json:"reason,omitempty"`
}

func (m DLQMessage) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal DLQMessage: %w", err)
	}
	return bytes, nil
}

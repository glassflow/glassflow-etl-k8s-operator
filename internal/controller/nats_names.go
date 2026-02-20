/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// PipelineStreamPrefix is the NATS stream/subject prefix (aligned with glassflow-api).
const PipelineStreamPrefix = "gf"

// MaxStreamNameLength is the maximum NATS stream name length (aligned with glassflow-api).
const MaxStreamNameLength = 256

// generateStreamHash returns the first 8 hex chars of SHA256(pipelineID), aligned with glassflow-api GenerateStreamHash.
func generateStreamHash(pipelineID string) string {
	hash := sha256.Sum256([]byte(pipelineID))
	return hex.EncodeToString(hash[:])[:8]
}

// sanitizeNATSSubject replaces dots with underscores in topic names for NATS (aligned with glassflow-api SanitizeNATSSubject).
func sanitizeNATSSubject(topicName string) string {
	return strings.ReplaceAll(topicName, ".", "_")
}

// getIngestorSubjectPrefix returns the subject prefix for ingestor output (aligned with glassflow-api GetIngestorStreamName).
// Ingestor publishes to subjectPrefix.POD_INDEX (e.g. gf-abc12345-topic.0).
func getIngestorSubjectPrefix(pipelineID, topicName string) string {
	hash := generateStreamHash(pipelineID)
	sanitized := sanitizeNATSSubject(topicName)
	streamName := fmt.Sprintf("%s-%s-%s", PipelineStreamPrefix, hash, sanitized)
	if len(streamName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s-", PipelineStreamPrefix, hash)
		maxTopicLength := MaxStreamNameLength - len(prefix)
		if maxTopicLength > 0 {
			streamName = prefix + sanitized[:min(len(sanitized), maxTopicLength)]
		} else {
			streamName = prefix[:MaxStreamNameLength]
		}
	}
	return streamName
}

// getSinkInputStreamPrefix returns the stream name prefix for sink input streams (aligned with glassflow-api GetNATSSinkConsumerName).
// Sink pod index j consumes from stream prefix_j (e.g. gf-abc12345-sink_0).
func getSinkInputStreamPrefix(pipelineID string) string {
	hash := generateStreamHash(pipelineID)
	return fmt.Sprintf("%s-%s-sink", PipelineStreamPrefix, hash)
}

// getDedupInputStreamPrefix returns the stream name prefix for the D streams that dedup reads from.
// Stream names are prefix_0, ..., prefix_(D-1). Same length/truncation rules as getIngestorSubjectPrefix.
func getDedupInputStreamPrefix(pipelineID, topicName string) string {
	hash := generateStreamHash(pipelineID)
	sanitized := sanitizeNATSSubject(topicName)
	streamName := fmt.Sprintf("%s-%s-%s-dedup-in", PipelineStreamPrefix, hash, sanitized)
	if len(streamName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s-", PipelineStreamPrefix, hash)
		maxTopicLength := MaxStreamNameLength - len(prefix) - len("-dedup-in")
		if maxTopicLength > 0 {
			streamName = prefix + sanitized[:min(len(sanitized), maxTopicLength)] + "-dedup-in"
		} else {
			streamName = (prefix + "dedup-in")[:MaxStreamNameLength]
		}
	}
	return streamName
}

// getDedupOutputSubjectPrefix returns the subject prefix for dedup output. Dedup pod d publishes to prefix.d.
// Same length/truncation rules as getIngestorSubjectPrefix.
func getDedupOutputSubjectPrefix(pipelineID, topicName string) string {
	hash := generateStreamHash(pipelineID)
	sanitized := sanitizeNATSSubject(topicName)
	streamName := fmt.Sprintf("%s-%s-%s-dedup-out", PipelineStreamPrefix, hash, sanitized)
	if len(streamName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s-", PipelineStreamPrefix, hash)
		maxTopicLength := MaxStreamNameLength - len(prefix) - len("-dedup-out")
		if maxTopicLength > 0 {
			streamName = prefix + sanitized[:min(len(sanitized), maxTopicLength)] + "-dedup-out"
		} else {
			streamName = (prefix + "dedup-out")[:MaxStreamNameLength]
		}
	}
	return streamName
}

// getSubjectsForStreamIndex returns the list of subjects that map to stream index s (round-robin: subject i -> stream i%N).
// subjectPrefix is the ingestor subject prefix; subjects are subjectPrefix.0, subjectPrefix.1, ... subjectPrefix.(M-1).
func getSubjectsForStreamIndex(subjectPrefix string, M, N, streamIndex int) []string {
	if N <= 0 || streamIndex < 0 || streamIndex >= N {
		return nil
	}
	var subjects []string
	for i := streamIndex; i < M; i += N {
		subjects = append(subjects, subjectPrefix+"."+strconv.Itoa(i))
	}
	return subjects
}

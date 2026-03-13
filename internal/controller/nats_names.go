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
	"strings"
)

// PipelineStreamPrefix is the NATS stream/subject prefix (aligned with glassflow-api).
const PipelineStreamPrefix = "gfm"

// MaxStreamNameLength is the maximum NATS stream name length (aligned with glassflow-api).
const MaxStreamNameLength = 256

// NATSConsumerNamePrefix is the NATS consumer name prefix (aligned with glassflow-api).
const NATSConsumerNamePrefix = "gf-nats"

// DLQSuffix is the DLQ stream suffix (aligned with glassflow-api).
const DLQSuffix = "DLQ"

// generatePipelineHash returns the first 8 hex chars of SHA256(pipelineID), aligned with glassflow-api GenerateStreamHash.
func generatePipelineHash(pipelineID string) string {
	hash := sha256.Sum256([]byte(pipelineID))
	return hex.EncodeToString(hash[:])[:8]
}

// sanitizeNATSSubject replaces dots with underscores in topic names for NATS (aligned with glassflow-api SanitizeNATSSubject).
func sanitizeNATSSubject(topicName string) string {
	return strings.ReplaceAll(topicName, ".", "_")
}

// getIngestorOutputSubjectPrefix returns the subject prefix for ingestor output (aligned with glassflow-api GetIngestorStreamName).
// Ingestor publishes to subjectPrefix.POD_INDEX (e.g. gf-abc12345-topic.0).
func getIngestorOutputSubjectPrefix(pipelineID, topicName string) string {
	hash := generatePipelineHash(pipelineID)
	sanitized := sanitizeNATSSubject(topicName)
	subjectName := fmt.Sprintf("%s-%s-%s", PipelineStreamPrefix, hash, sanitized)
	if len(subjectName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s-", PipelineStreamPrefix, hash)
		maxTopicLength := MaxStreamNameLength - len(prefix)
		if maxTopicLength > 0 {
			subjectName = prefix + sanitized[:min(len(sanitized), maxTopicLength)]
		} else {
			subjectName = prefix[:MaxStreamNameLength]
		}
	}
	return subjectName
}

// getSinkInputStreamPrefix returns the stream name prefix for sink input streams (aligned with glassflow-api GetNATSSinkConsumerName).
// Sink pod index j consumes from stream prefix_j (e.g. gf-abc12345-sink_0).
func getSinkInputStreamPrefix(pipelineID string) string {
	hash := generatePipelineHash(pipelineID)
	return fmt.Sprintf("%s-%s-sink", PipelineStreamPrefix, hash)
}

// getDedupInputStreamPrefix returns the stream name prefix for the dedupReplicas streams that dedup reads from.
// Stream names are prefix_0, ..., prefix_(dedupReplicas-1). Same length/truncation rules as getIngestorOutputSubjectPrefix.
func getDedupInputStreamPrefix(pipelineID, topicName string) string {
	hash := generatePipelineHash(pipelineID)
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
// Same length/truncation rules as getIngestorOutputSubjectPrefix.
func getDedupOutputSubjectPrefix(pipelineID, topicName string) string {
	hash := generatePipelineHash(pipelineID)
	sanitized := sanitizeNATSSubject(topicName)
	subjectName := fmt.Sprintf("%s-%s-%s-dedup-out", PipelineStreamPrefix, hash, sanitized)
	if len(subjectName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s-", PipelineStreamPrefix, hash)
		maxTopicLength := MaxStreamNameLength - len(prefix) - len("-dedup-out")
		if maxTopicLength > 0 {
			subjectName = prefix + sanitized[:min(len(sanitized), maxTopicLength)] + "-dedup-out"
		} else {
			subjectName = (prefix + "dedup-out")[:MaxStreamNameLength]
		}
	}
	return subjectName
}

// getJoinOutputSubjectPrefix returns the subject prefix for join output.
func getJoinOutputSubjectPrefix(pipelineID string) string {
	hash := generatePipelineHash(pipelineID)
	subjectName := fmt.Sprintf("%s-%s-join-out", PipelineStreamPrefix, hash)
	if len(subjectName) > MaxStreamNameLength {
		prefix := fmt.Sprintf("%s-%s", PipelineStreamPrefix, hash)
		subjectName = (prefix + "join-out")[:MaxStreamNameLength]
	}
	return subjectName
}

func getDLQStreamName(pipelineID string) string {
	hash := generatePipelineHash(pipelineID)
	return fmt.Sprintf("%s-%s-%s", PipelineStreamPrefix, hash, DLQSuffix)
}

// getNATSConsumerName builds consumer names aligned with glassflow-api GetNATSConsumerName.
func getNATSConsumerName(pipelineID, componentType, streamType string) string {
	componentAbbr := map[string]string{
		"sink":  "s",
		"join":  "j",
		"dedup": "d",
	}
	streamAbbr := map[string]string{
		"input": "i",
		"left":  "l",
		"right": "r",
	}
	return fmt.Sprintf("%s-%s%s-%s",
		NATSConsumerNamePrefix,
		componentAbbr[componentType],
		streamAbbr[streamType],
		generatePipelineHash(pipelineID))
}

func getNATSSinkConsumerName(pipelineID string) string {
	return getNATSConsumerName(pipelineID, "sink", "input")
}

func getNATSJoinLeftConsumerName(pipelineID string) string {
	return getNATSConsumerName(pipelineID, "join", "left")
}

func getNATSJoinRightConsumerName(pipelineID string) string {
	return getNATSConsumerName(pipelineID, "join", "right")
}

func getNATSDedupConsumerName(pipelineID string) string {
	return getNATSConsumerName(pipelineID, "dedup", "input")
}

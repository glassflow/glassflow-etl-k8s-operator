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
)

// PipelineStreamPrefix is the NATS stream/subject prefix (aligned with glassflow-api).
const PipelineStreamPrefix = "gfm"

// NATSConsumerNamePrefix is the NATS consumer name prefix (aligned with glassflow-api).
const NATSConsumerNamePrefix = "gf-nats"

// DLQSuffix is the DLQ stream suffix (aligned with glassflow-api).
const DLQSuffix = "DLQ"

// generatePipelineHash returns the first 8 hex chars of SHA256(pipelineID),
// aligned with glassflow-api GenerateStreamHash.
func generatePipelineHash(pipelineID string) string {
	hash := sha256.Sum256([]byte(pipelineID))
	return hex.EncodeToString(hash[:])[:8]
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

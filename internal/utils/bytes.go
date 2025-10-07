package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseBytes parses a byte string that can be either a number or human-readable format (e.g., "100GB", "1TB")
func ParseBytes(s string) (int64, error) {
	// First try to parse as a plain number
	if bytes, err := strconv.ParseInt(s, 10, 64); err == nil {
		return bytes, nil
	}

	// Handle human-readable formats
	s = strings.ToUpper(strings.TrimSpace(s))

	var multiplier int64
	var numStr string

	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "TB")
	} else {
		return 0, fmt.Errorf("invalid byte format: %s", s)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in byte format: %s", s)
	}

	return int64(num * float64(multiplier)), nil
}

// ConvertBytesToString converts human-readable byte format to numeric string for components
func ConvertBytesToString(bytesStr string) string {
	// First try to parse as a plain number
	if _, err := strconv.ParseInt(bytesStr, 10, 64); err == nil {
		return bytesStr // Already a numeric string
	}

	// Handle human-readable formats
	s := strings.ToUpper(strings.TrimSpace(bytesStr))

	var multiplier int64
	var numStr string

	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "TB")
	} else {
		// If we can't parse it, return the default
		return "107374182400" // 100GB default
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		// If we can't parse the number, return the default
		return "107374182400" // 100GB default
	}

	return strconv.FormatInt(int64(num*float64(multiplier)), 10)
}

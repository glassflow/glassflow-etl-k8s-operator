package controller

import (
	"testing"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
)

func TestGetPipelineOperationFromAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		annotations map[string]string
		expected    string
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			expected:    "",
		},
		{
			name:        "create",
			annotations: map[string]string{constants.PipelineCreateAnnotation: "true"},
			expected:    constants.OperationCreate,
		},
		{
			name:        "resume",
			annotations: map[string]string{constants.PipelineResumeAnnotation: "true"},
			expected:    constants.OperationResume,
		},
		{
			name:        "stop",
			annotations: map[string]string{constants.PipelineStopAnnotation: "true"},
			expected:    constants.OperationStop,
		},
		{
			name:        "edit",
			annotations: map[string]string{constants.PipelineEditAnnotation: "true"},
			expected:    constants.OperationEdit,
		},
		{
			name:        "terminate",
			annotations: map[string]string{constants.PipelineTerminateAnnotation: "true"},
			expected:    constants.OperationTerminate,
		},
		{
			name:        "delete",
			annotations: map[string]string{constants.PipelineDeleteAnnotation: "true"},
			expected:    constants.OperationDelete,
		},
		{
			name:        "helm uninstall",
			annotations: map[string]string{constants.PipelineHelmUninstallAnnotation: "true"},
			expected:    constants.OperationHelmUninstall,
		},
		{
			name: "priority prefers helm uninstall over all others",
			annotations: map[string]string{
				constants.PipelineCreateAnnotation:        "true",
				constants.PipelineResumeAnnotation:        "true",
				constants.PipelineStopAnnotation:          "true",
				constants.PipelineEditAnnotation:          "true",
				constants.PipelineTerminateAnnotation:     "true",
				constants.PipelineDeleteAnnotation:        "true",
				constants.PipelineHelmUninstallAnnotation: "true",
			},
			expected: constants.OperationHelmUninstall,
		},
		{
			name: "priority prefers delete over terminate/create",
			annotations: map[string]string{
				constants.PipelineCreateAnnotation:    "true",
				constants.PipelineTerminateAnnotation: "true",
				constants.PipelineDeleteAnnotation:    "true",
			},
			expected: constants.OperationDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getPipelineOperationFromAnnotations(tt.annotations)
			if got != tt.expected {
				t.Fatalf("getPipelineOperationFromAnnotations() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestClearOperationAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		operation  string
		annotation string
	}{
		{
			name:       "create",
			operation:  constants.OperationCreate,
			annotation: constants.PipelineCreateAnnotation,
		},
		{
			name:       "resume",
			operation:  constants.OperationResume,
			annotation: constants.PipelineResumeAnnotation,
		},
		{
			name:       "stop",
			operation:  constants.OperationStop,
			annotation: constants.PipelineStopAnnotation,
		},
		{
			name:       "edit",
			operation:  constants.OperationEdit,
			annotation: constants.PipelineEditAnnotation,
		},
		{
			name:       "terminate",
			operation:  constants.OperationTerminate,
			annotation: constants.PipelineTerminateAnnotation,
		},
		{
			name:       "delete",
			operation:  constants.OperationDelete,
			annotation: constants.PipelineDeleteAnnotation,
		},
		{
			name:       "helm uninstall",
			operation:  constants.OperationHelmUninstall,
			annotation: constants.PipelineHelmUninstallAnnotation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			annotations := map[string]string{
				tt.annotation: "true",
				"other":       "keep",
			}

			clearOperationAnnotation(annotations, tt.operation)

			if _, exists := annotations[tt.annotation]; exists {
				t.Fatalf("clearOperationAnnotation() did not remove annotation %q", tt.annotation)
			}
			if annotations["other"] != "keep" {
				t.Fatalf("clearOperationAnnotation() removed unrelated annotation")
			}
		})
	}
}

package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
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

func TestGetStopLastPendingCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		annotations map[string]string
		wantCount   int
		wantOK      bool
	}{
		{
			name:        "no annotations",
			annotations: nil,
			wantOK:      false,
		},
		{
			name:        "annotation absent",
			annotations: map[string]string{"other": "value"},
			wantOK:      false,
		},
		{
			name:        "annotation present with valid count",
			annotations: map[string]string{constants.PipelineStopLastPendingCountAnnotation: "42"},
			wantCount:   42,
			wantOK:      true,
		},
		{
			name:        "annotation present with zero",
			annotations: map[string]string{constants.PipelineStopLastPendingCountAnnotation: "0"},
			wantCount:   0,
			wantOK:      true,
		},
		{
			name:        "annotation present with invalid value",
			annotations: map[string]string{constants.PipelineStopLastPendingCountAnnotation: "not-a-number"},
			wantOK:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &etlv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations},
			}

			count, ok := getStopLastPendingCount(p)
			if ok != tt.wantOK {
				t.Fatalf("getStopLastPendingCount() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && count != tt.wantCount {
				t.Fatalf("getStopLastPendingCount() count = %d, want %d", count, tt.wantCount)
			}
		})
	}
}

func TestSetStopLastPendingCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, p := newOperationTestReconcilerWithPipeline(t, map[string]string{
		constants.PipelineStopAnnotation: "true",
	})

	if err := r.setStopLastPendingCount(ctx, &p, 123); err != nil {
		t.Fatalf("setStopLastPendingCount() returned unexpected error: %v", err)
	}

	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	count, ok := getStopLastPendingCount(stored)
	if !ok {
		t.Fatalf("setStopLastPendingCount() did not set annotation")
	}
	if count != 123 {
		t.Fatalf("setStopLastPendingCount() count = %d, want 123", count)
	}
}

func TestClearStopLastPendingCount(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	p := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				constants.PipelineStopLastPendingCountAnnotation: "99",
				"keep": "value",
			},
		},
	}

	r.clearStopLastPendingCount(p)

	if _, exists := p.GetAnnotations()[constants.PipelineStopLastPendingCountAnnotation]; exists {
		t.Fatalf("clearStopLastPendingCount() did not remove annotation")
	}
	if p.GetAnnotations()["keep"] != "value" {
		t.Fatalf("clearStopLastPendingCount() removed unrelated annotation")
	}
}

func TestExtendOperationTimeout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	before := time.Now().UTC().Truncate(time.Second)

	r, p := newOperationTestReconcilerWithPipeline(t, map[string]string{
		constants.PipelineOperationStartTimeAnnotation: "2020-01-01T00:00:00Z",
	})

	if err := r.extendOperationTimeout(ctx, &p); err != nil {
		t.Fatalf("extendOperationTimeout() returned unexpected error: %v", err)
	}

	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	newStartStr, exists := stored.Annotations[constants.PipelineOperationStartTimeAnnotation]
	if !exists {
		t.Fatalf("extendOperationTimeout() did not set operation start time annotation")
	}

	newStart, err := time.Parse(time.RFC3339, newStartStr)
	if err != nil {
		t.Fatalf("extendOperationTimeout() stored unparseable timestamp: %v", err)
	}
	if newStart.Before(before) {
		t.Fatalf("extendOperationTimeout() new start time %v is before test start %v", newStart, before)
	}
	if newStart.Equal(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("extendOperationTimeout() did not update the start time")
	}
}

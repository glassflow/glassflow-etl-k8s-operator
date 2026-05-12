package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
	natspkg "github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

// fakePipelineStorage captures status updates for assertions.
type fakePipelineStorage struct {
	mu      sync.Mutex
	updates []pipelineStatusUpdate
}

type pipelineStatusUpdate struct {
	pipelineID string
	status     models.PipelineStatus
	errors     []string
	reason     string
}

func (f *fakePipelineStorage) UpdatePipelineStatus(_ context.Context, pipelineID string, status models.PipelineStatus, errs []string, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, pipelineStatusUpdate{
		pipelineID: pipelineID,
		status:     status,
		errors:     append([]string(nil), errs...),
		reason:     reason,
	})
	return nil
}

func (f *fakePipelineStorage) DeletePipeline(_ context.Context, _ string) error { return nil }

func (f *fakePipelineStorage) snapshot() []pipelineStatusUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]pipelineStatusUpdate, len(f.updates))
	copy(out, f.updates)
	return out
}

func registerEtlScheme(t *testing.T) {
	t.Helper()
	if err := etlv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("register etlv1alpha1 scheme: %v", err)
	}
}

// expiredStartTime returns a timestamp far enough in the past that checkOperationTimeout
// would deem any sane operation timed out.
func expiredStartTime() string {
	return time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
}

// TestHandleOperationTimeoutStopRunsSweep covers the new Option A branch: when a stop
// operation times out we still run the sweep + cleanup tail after force-terminating
// components. Pipeline has no Source.Streams so buildNATSResourcePlan deterministically
// errors inside sweepMessagesToDLQ — that exercises the sweep-failure side of the
// branch (Failed with the sweep error captured in storage) without needing live NATS.
func TestHandleOperationTimeoutStopRunsSweep(t *testing.T) {
	registerEtlScheme(t)

	pipeline := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipe-stop",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineStopAnnotation:               "true",
				constants.PipelineOperationStartTimeAnnotation: expiredStartTime(),
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID:     "pipe-stop-id",
			Source: etlv1alpha1.Sources{Type: "kafka"},
			Sink:   etlv1alpha1.Sink{Type: "clickhouse"},
		},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusStopping),
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(pipeline).
		WithStatusSubresource(&etlv1alpha1.Pipeline{}).
		Build()

	storage := &fakePipelineStorage{}
	r := &PipelineReconciler{
		Client:          c,
		Scheme:          scheme.Scheme,
		NATSClient:      &natspkg.NATSClient{},
		PostgresStorage: storage,
	}

	if _, err := r.handleOperationTimeout(context.Background(), logr.Discard(), pipeline); err != nil {
		t.Fatalf("handleOperationTimeout returned error: %v", err)
	}

	var stored etlv1alpha1.Pipeline
	if err := c.Get(context.Background(), types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}, &stored); err != nil {
		t.Fatalf("read back pipeline: %v", err)
	}

	if stored.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		t.Fatalf("status = %q, want %q (sweep failed, so stop tail must mark Failed)",
			stored.Status, models.PipelineStatusFailed)
	}
	if _, exists := stored.Annotations[constants.PipelineStopAnnotation]; exists {
		t.Fatalf("stop annotation was not cleared")
	}
	if _, exists := stored.Annotations[constants.PipelineOperationStartTimeAnnotation]; exists {
		t.Fatalf("operation start time annotation was not cleared")
	}

	updates := storage.snapshot()
	// Stop must NOT pre-emit a generic timeout Failed update — that's the old behavior the
	// Option A change removed so we don't briefly publish Failed before Stopped.
	for _, u := range updates {
		if u.reason == "timeout" && len(u.errors) > 0 &&
			strings.Contains(u.errors[0], "operation timed out") {
			t.Fatalf("stop op emitted up-front timeout Failed update; updates=%+v", updates)
		}
	}
	// We expect a Failed update whose error message names the sweep failure.
	matched := false
	for _, u := range updates {
		if u.status == models.PipelineStatusFailed {
			for _, e := range u.errors {
				if strings.Contains(e, "forced stop sweep failed") {
					matched = true
				}
			}
		}
	}
	if !matched {
		t.Fatalf("expected a Failed update mentioning 'forced stop sweep failed'; updates=%+v", updates)
	}
}

// TestHandleOperationTimeoutNonStopMarksFailedUpFront guards the existing behavior for
// non-stop operations: status flips to Failed before terminate, no forced-stop branch
// fires (no sweep/cleanup attempted).
func TestHandleOperationTimeoutNonStopMarksFailedUpFront(t *testing.T) {
	registerEtlScheme(t)

	pipeline := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipe-edit",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineEditAnnotation:               "true",
				constants.PipelineOperationStartTimeAnnotation: expiredStartTime(),
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID:     "pipe-edit-id",
			Source: etlv1alpha1.Sources{Type: "kafka"},
			Sink:   etlv1alpha1.Sink{Type: "clickhouse"},
		},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusRunning),
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(pipeline).
		WithStatusSubresource(&etlv1alpha1.Pipeline{}).
		Build()

	storage := &fakePipelineStorage{}
	r := &PipelineReconciler{
		Client:          c,
		Scheme:          scheme.Scheme,
		NATSClient:      &natspkg.NATSClient{},
		PostgresStorage: storage,
	}

	if _, err := r.handleOperationTimeout(context.Background(), logr.Discard(), pipeline); err != nil {
		t.Fatalf("handleOperationTimeout returned error: %v", err)
	}

	var stored etlv1alpha1.Pipeline
	if err := c.Get(context.Background(), types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}, &stored); err != nil {
		t.Fatalf("read back pipeline: %v", err)
	}

	if stored.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		t.Fatalf("status = %q, want Failed", stored.Status)
	}
	if _, exists := stored.Annotations[constants.PipelineEditAnnotation]; exists {
		t.Fatalf("edit annotation was not cleared")
	}

	updates := storage.snapshot()
	// Non-stop ops must still emit the generic timeout Failed update.
	foundTimeout := false
	for _, u := range updates {
		if u.reason == "timeout" && u.status == models.PipelineStatusFailed {
			foundTimeout = true
		}
		for _, e := range u.errors {
			if strings.Contains(e, "forced stop") {
				t.Fatalf("non-stop op unexpectedly hit forced-stop branch; updates=%+v", updates)
			}
		}
	}
	if !foundTimeout {
		t.Fatalf("expected a timeout Failed update for non-stop op; updates=%+v", updates)
	}
}

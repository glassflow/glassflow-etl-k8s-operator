package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
)

func TestOperationHandlerForKnownOperations(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}

	operations := []string{
		constants.OperationHelmUninstall,
		constants.OperationCreate,
		constants.OperationResume,
		constants.OperationStop,
		constants.OperationEdit,
		constants.OperationTerminate,
		constants.OperationDelete,
	}

	for _, operation := range operations {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			handler := r.operationHandlerFor(operation)
			if handler == nil {
				t.Fatalf("operationHandlerFor(%q) returned nil handler", operation)
			}
		})
	}
}

func TestOperationHandlerForUnknownOperation(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	if handler := r.operationHandlerFor("unknown-operation"); handler != nil {
		t.Fatalf("operationHandlerFor() returned handler for unknown operation")
	}
}

func TestDispatchOperationUnknownOperationIsNoop(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	result, err := r.dispatchOperation(context.Background(), logr.Discard(), "unknown-operation", etlv1alpha1.Pipeline{})
	if err != nil {
		t.Fatalf("dispatchOperation() returned unexpected error: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("dispatchOperation() returned unexpected result: %#v", result)
	}
}

func TestDefaultOperationRequeueResult(t *testing.T) {
	t.Parallel()

	result := defaultOperationRequeueResult()
	if !result.Requeue {
		t.Fatalf("defaultOperationRequeueResult() expected Requeue=true, got %#v", result)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("defaultOperationRequeueResult() expected RequeueAfter=%s, got %s", time.Second, result.RequeueAfter)
	}
}

func TestSetOperationStartTimeBestEffortSetsOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, p := newOperationTestReconcilerWithPipeline(t, map[string]string{
		constants.PipelineCreateAnnotation: "true",
	})

	r.setOperationStartTimeBestEffort(ctx, logr.Discard(), &p)
	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	firstStartTime, exists := stored.Annotations[constants.PipelineOperationStartTimeAnnotation]
	if !exists || firstStartTime == "" {
		t.Fatalf("setOperationStartTimeBestEffort() did not set operation start time annotation")
	}

	r.setOperationStartTimeBestEffort(ctx, logr.Discard(), stored)
	storedAgain := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	secondStartTime := storedAgain.Annotations[constants.PipelineOperationStartTimeAnnotation]
	if secondStartTime != firstStartTime {
		t.Fatalf("setOperationStartTimeBestEffort() overwrote operation start time annotation")
	}
}

func TestSetOperationStartTimeBestEffortHandlesUpdateError(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{
		Client: &operationTestClient{
			objects: map[types.NamespacedName]*etlv1alpha1.Pipeline{},
		},
	}

	p := etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "missing-pipeline",
			Namespace: "default",
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID: "missing-pipeline-id",
		},
	}

	r.setOperationStartTimeBestEffort(context.Background(), logr.Discard(), &p)

	startTime := p.GetAnnotations()[constants.PipelineOperationStartTimeAnnotation]
	if startTime == "" {
		t.Fatalf("setOperationStartTimeBestEffort() did not set local operation start time on update error")
	}
}

func TestClearOperationAnnotationAndStatusClearsStartTimeWhenRequested(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, p := newOperationTestReconcilerWithPipeline(t, map[string]string{
		constants.PipelineCreateAnnotation:             "true",
		constants.PipelineOperationStartTimeAnnotation: "2026-03-16T12:00:00Z",
		"keep": "value",
	})

	r.clearOperationAnnotationAndStatus(
		ctx,
		logr.Discard(),
		&p,
		constants.PipelineCreateAnnotation,
		models.PipelineStatusRunning,
		true,
	)

	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	if _, exists := stored.Annotations[constants.PipelineCreateAnnotation]; exists {
		t.Fatalf("clearOperationAnnotationAndStatus() did not remove create annotation")
	}
	if _, exists := stored.Annotations[constants.PipelineOperationStartTimeAnnotation]; exists {
		t.Fatalf("clearOperationAnnotationAndStatus() did not clear operation start time")
	}
	if stored.Annotations["keep"] != "value" {
		t.Fatalf("clearOperationAnnotationAndStatus() modified unrelated annotations")
	}
	if stored.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusRunning) {
		t.Fatalf("clearOperationAnnotationAndStatus() status = %q, want %q", stored.Status, models.PipelineStatusRunning)
	}
}

func TestClearOperationAnnotationAndStatusPreservesStartTimeWhenNotRequested(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, p := newOperationTestReconcilerWithPipeline(t, map[string]string{
		constants.PipelineTerminateAnnotation:          "true",
		constants.PipelineOperationStartTimeAnnotation: "2026-03-16T12:00:00Z",
	})

	r.clearOperationAnnotationAndStatus(
		ctx,
		logr.Discard(),
		&p,
		constants.PipelineTerminateAnnotation,
		models.PipelineStatusStopped,
		false,
	)

	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	if _, exists := stored.Annotations[constants.PipelineTerminateAnnotation]; exists {
		t.Fatalf("clearOperationAnnotationAndStatus() did not remove terminate annotation")
	}
	if _, exists := stored.Annotations[constants.PipelineOperationStartTimeAnnotation]; !exists {
		t.Fatalf("clearOperationAnnotationAndStatus() unexpectedly removed operation start time")
	}
	if stored.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		t.Fatalf("clearOperationAnnotationAndStatus() status = %q, want %q", stored.Status, models.PipelineStatusStopped)
	}
}

func TestClearOperationAnnotationAndStatusNoAnnotationsNoop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, p := newOperationTestReconcilerWithPipeline(t, nil)

	r.clearOperationAnnotationAndStatus(
		ctx,
		logr.Discard(),
		&p,
		constants.PipelineEditAnnotation,
		models.PipelineStatusRunning,
		true,
	)

	stored := getPipelineForOperationTest(t, r, p.Name, p.Namespace)
	if stored.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusCreated) {
		t.Fatalf("clearOperationAnnotationAndStatus() unexpectedly changed status for nil annotations")
	}
}

func TestClearOperationAnnotationAndStatusHandlesUpdateError(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{
		Client: &operationTestClient{
			objects: map[types.NamespacedName]*etlv1alpha1.Pipeline{},
		},
	}

	p := etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "missing-pipeline",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineStopAnnotation: "true",
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID: "missing-pipeline-id",
		},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusStopping),
	}

	r.clearOperationAnnotationAndStatus(
		context.Background(),
		logr.Discard(),
		&p,
		constants.PipelineStopAnnotation,
		models.PipelineStatusStopped,
		true,
	)

	if _, exists := p.GetAnnotations()[constants.PipelineStopAnnotation]; exists {
		t.Fatalf("clearOperationAnnotationAndStatus() did not remove local annotation on update error")
	}
	if p.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		t.Fatalf("clearOperationAnnotationAndStatus() did not set local status on update error")
	}
}

func TestCheckOperationTimeoutAndLogProgressNoTimeout(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	p := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipeline",
			Namespace: "default",
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipeline-id",
		},
	}

	result, handled, err := r.checkOperationTimeoutAndLogProgress(context.Background(), logr.Discard(), p)
	if err != nil {
		t.Fatalf("checkOperationTimeoutAndLogProgress() returned error: %v", err)
	}
	if handled {
		t.Fatalf("checkOperationTimeoutAndLogProgress() unexpectedly handled timeout")
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("checkOperationTimeoutAndLogProgress() returned unexpected result: %#v", result)
	}
}

func TestRecordOperationSuccessNoopWhenObserversDisabled(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	r.recordOperationSuccess(context.Background(), "create", "create", "pipeline-id")
}

func newOperationTestReconcilerWithPipeline(t *testing.T, annotations map[string]string) (*PipelineReconciler, etlv1alpha1.Pipeline) {
	t.Helper()

	pipeline := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pipeline-test",
			Namespace:   "default",
			Annotations: annotations,
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipeline-test-id",
		},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusCreated),
	}

	c := &operationTestClient{
		objects: map[types.NamespacedName]*etlv1alpha1.Pipeline{
			{Name: pipeline.Name, Namespace: pipeline.Namespace}: pipeline.DeepCopy(),
		},
	}

	r := &PipelineReconciler{
		Client: c,
	}

	loaded := getPipelineForOperationTest(t, r, pipeline.Name, pipeline.Namespace)
	return r, *loaded
}

func getPipelineForOperationTest(t *testing.T, r *PipelineReconciler, name, namespace string) *etlv1alpha1.Pipeline {
	t.Helper()

	var pipeline etlv1alpha1.Pipeline
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &pipeline); err != nil {
		t.Fatalf("failed to get pipeline: %v", err)
	}
	return &pipeline
}

type operationTestClient struct {
	client.Client
	mu      sync.Mutex
	objects map[types.NamespacedName]*etlv1alpha1.Pipeline
}

func (c *operationTestClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	pipeline, ok := obj.(*etlv1alpha1.Pipeline)
	if !ok {
		return fmt.Errorf("unsupported object type %T", obj)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	stored, exists := c.objects[types.NamespacedName{Name: key.Name, Namespace: key.Namespace}]
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: "etl.glassflow.io", Resource: "pipelines"}, key.Name)
	}

	*pipeline = *stored.DeepCopy()
	return nil
}

func (c *operationTestClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	pipeline, ok := obj.(*etlv1alpha1.Pipeline)
	if !ok {
		return fmt.Errorf("unsupported object type %T", obj)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}
	if _, exists := c.objects[key]; !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: "etl.glassflow.io", Resource: "pipelines"}, key.Name)
	}

	c.objects[key] = pipeline.DeepCopy()
	return nil
}

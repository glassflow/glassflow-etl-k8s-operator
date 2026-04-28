package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
)

// helmUninstallTestClient is a minimal fake client for helm-uninstall tests.
type helmUninstallTestClient struct {
	client.Client
	mu      sync.Mutex
	objects map[types.NamespacedName]*etlv1alpha1.Pipeline
	deleted []string
}

func (c *helmUninstallTestClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	pipeline, ok := obj.(*etlv1alpha1.Pipeline)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, exists := c.objects[key]
	if !exists {
		return nil
	}
	*pipeline = *stored.DeepCopy()
	return nil
}

func (c *helmUninstallTestClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	pipeline, ok := obj.(*etlv1alpha1.Pipeline)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}
	c.objects[key] = pipeline.DeepCopy()
	return nil
}

func (c *helmUninstallTestClient) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	pipeline, ok := obj.(*etlv1alpha1.Pipeline)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}
	delete(c.objects, key)
	c.deleted = append(c.deleted, pipeline.Spec.ID)
	return nil
}

// fakeStorage records DeletePipeline calls and can simulate errors.
type fakeStorage struct {
	mu        sync.Mutex
	deleted   []string
	returnErr error
}

func (f *fakeStorage) DeletePipeline(_ context.Context, pipelineID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.returnErr != nil {
		return f.returnErr
	}
	f.deleted = append(f.deleted, pipelineID)
	return nil
}

func (f *fakeStorage) UpdatePipelineStatus(_ context.Context, _ string, _ models.PipelineStatus, _ []string, _ string) error {
	return nil
}

func TestReconcileHelmUninstallDeletesStoppedPipelineFromPostgres(t *testing.T) {
	t.Parallel()

	const pipelineID = "stopped-pipeline-id"

	pipeline := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stopped-pipeline",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineHelmUninstallAnnotation: "true",
			},
			Finalizers: []string{constants.PipelineFinalizerName},
		},
		Spec:   etlv1alpha1.PipelineSpec{ID: pipelineID},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusStopped),
	}

	fakeClient := &helmUninstallTestClient{
		objects: map[types.NamespacedName]*etlv1alpha1.Pipeline{
			{Name: pipeline.Name, Namespace: pipeline.Namespace}: pipeline.DeepCopy(),
		},
	}
	storage := &fakeStorage{}

	r := &PipelineReconciler{
		Client:          fakeClient,
		PostgresStorage: storage,
	}

	_, err := r.reconcileHelmUninstall(context.Background(), logr.Discard(), *pipeline)
	if err != nil {
		t.Fatalf("reconcileHelmUninstall() returned unexpected error: %v", err)
	}

	storage.mu.Lock()
	deletedIDs := storage.deleted
	storage.mu.Unlock()

	if len(deletedIDs) != 1 || deletedIDs[0] != pipelineID {
		t.Fatalf("expected postgres DeletePipeline(%q), got deletedIDs=%v", pipelineID, deletedIDs)
	}

	fakeClient.mu.Lock()
	deletedCRDs := fakeClient.deleted
	fakeClient.mu.Unlock()

	if len(deletedCRDs) != 1 || deletedCRDs[0] != pipelineID {
		t.Fatalf("expected CRD to be deleted, got deletedCRDs=%v", deletedCRDs)
	}
}

func TestReconcileHelmUninstallStoppedPipelineDeleteErrorIsNonFatal(t *testing.T) {
	t.Parallel()

	const pipelineID = "stopped-pipeline-delete-error"

	pipeline := &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stopped-pipeline-2",
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineHelmUninstallAnnotation: "true",
			},
			Finalizers: []string{constants.PipelineFinalizerName},
		},
		Spec:   etlv1alpha1.PipelineSpec{ID: pipelineID},
		Status: etlv1alpha1.PipelineStatus(models.PipelineStatusStopped),
	}

	fakeClient := &helmUninstallTestClient{
		objects: map[types.NamespacedName]*etlv1alpha1.Pipeline{
			{Name: pipeline.Name, Namespace: pipeline.Namespace}: pipeline.DeepCopy(),
		},
	}
	storage := &fakeStorage{returnErr: fmt.Errorf("postgres unavailable")}

	r := &PipelineReconciler{
		Client:          fakeClient,
		PostgresStorage: storage,
	}

	_, err := r.reconcileHelmUninstall(context.Background(), logr.Discard(), *pipeline)
	if err != nil {
		t.Fatalf("reconcileHelmUninstall() should not return error when postgres delete fails: %v", err)
	}

	// CRD should still be deleted even if postgres failed
	fakeClient.mu.Lock()
	deletedCRDs := fakeClient.deleted
	fakeClient.mu.Unlock()

	if len(deletedCRDs) != 1 || deletedCRDs[0] != pipelineID {
		t.Fatalf("expected CRD to be deleted even on postgres error, got deletedCRDs=%v", deletedCRDs)
	}
}

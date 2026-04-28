package controller

import (
	"context"
	"testing"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCleanupDedupPVCsRunsWhenDedupStorageDisabled(t *testing.T) {
	t.Parallel()

	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipeline-1",
			Source: etlv1alpha1.Sources{
				Streams: []etlv1alpha1.SourceStream{
					{
						TopicName:     "topic-a",
						Deduplication: &etlv1alpha1.Deduplication{Enabled: false},
					},
				},
			},
			Transform: etlv1alpha1.Transform{
				IsDedupEnabled: false,
			},
		},
	}

	namespace := "test-ns"
	reconciler := &PipelineReconciler{
		Client: fake.NewClientBuilder().Build(),
		Config: ReconcilerConfig{
			Namespaces: PipelineNamespaces{Name: namespace},
		},
	}

	// cleanupDedupPVCs should run (not skip) even when dedup is disabled.
	// With an empty fake client, there are no PVCs to delete — it should succeed.
	err := reconciler.cleanupDedupPVCs(context.Background(), logr.Discard(), pipeline)
	if err != nil {
		t.Fatalf("cleanupDedupPVCs() returned error: %v", err)
	}
}

func TestCleanupDisabledDedupPVCsSkipsEnabledStreams(t *testing.T) {
	t.Parallel()

	// Pipeline with dedup enabled — cleanupDisabledDedupPVCs should NOT delete the PVC.
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipeline-2",
			Source: etlv1alpha1.Sources{
				Streams: []etlv1alpha1.SourceStream{
					{
						TopicName:     "topic-a",
						Deduplication: &etlv1alpha1.Deduplication{Enabled: true},
					},
				},
			},
			Transform: etlv1alpha1.Transform{IsDedupEnabled: true},
		},
	}

	namespace := "test-ns"
	pvcName := "data-gf-pipeline-2-dedup-0-0"

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}

	reconciler := &PipelineReconciler{
		Client: fake.NewClientBuilder().WithObjects(pvc).Build(),
		Config: ReconcilerConfig{
			Namespaces: PipelineNamespaces{Name: namespace},
		},
	}

	err := reconciler.cleanupDisabledDedupPVCs(context.Background(), logr.Discard(), pipeline)
	if err != nil {
		t.Fatalf("cleanupDisabledDedupPVCs() returned error: %v", err)
	}

	// PVC should still exist — dedup is enabled, so it should not be deleted.
	var remaining v1.PersistentVolumeClaim
	if err = reconciler.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pvcName}, &remaining); err != nil {
		t.Fatalf("expected PVC %s to still exist, got: %v", pvcName, err)
	}
}

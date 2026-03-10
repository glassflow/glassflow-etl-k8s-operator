package controller

import (
	"context"
	"testing"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/go-logr/logr"
)

func TestCleanupDedupPVCsSkipsWhenDedupStorageDisabled(t *testing.T) {
	t.Parallel()

	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipeline-1",
			Ingestor: etlv1alpha1.Sources{
				Streams: []etlv1alpha1.SourceStream{
					{
						TopicName:     "topic-a",
						Deduplication: &etlv1alpha1.Deduplication{Enabled: true},
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
		PipelinesNamespaceName: namespace,
	}

	err := reconciler.cleanupDedupPVCs(context.Background(), logr.Discard(), pipeline)
	if err != nil {
		t.Fatalf("cleanupDedupPVCs() returned error: %v", err)
	}
}

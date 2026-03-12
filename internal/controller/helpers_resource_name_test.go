package controller

import (
	"fmt"
	"strings"
	"testing"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
)

func TestGetStatefulSetResourceNameTruncatesPipelineID(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{PipelinesNamespaceAuto: false}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "abcdefghijklmnopqrstuvwxyz1234567890abcd", // 40 chars
		},
	}

	name := reconciler.getStatefulSetResourceName(pipeline, "ingestor-0")

	expectedPipelineIDLen := maxStatefulSetNameLengthForRevHash - (len(pipelineScopedResourcePrefix) + 1 + len("ingestor-0"))
	expectedPipelineID := pipeline.Spec.ID[:expectedPipelineIDLen]
	expectedName := fmt.Sprintf("%s%s-%s", pipelineScopedResourcePrefix, expectedPipelineID, "ingestor-0")
	if name != expectedName {
		t.Fatalf("unexpected truncated statefulset name: got=%q want=%q", name, expectedName)
	}

	if got, want := len(name), maxStatefulSetNameLengthForRevHash; got != want {
		t.Fatalf("unexpected statefulset name length: got=%d want=%d name=%q", got, want, name)
	}
	if !strings.HasSuffix(name, "-ingestor-0") {
		t.Fatalf("statefulset name must preserve base suffix, got %q", name)
	}

	// StatefulSet controller appends "-<10 chars>" for controller revisions.
	if got, want := len(name)+11, 63; got != want {
		t.Fatalf("controller-revision label value length must fit 63 chars: got=%d want=%d", got, want)
	}
}

func TestGetResourceNameDoesNotTruncateCurrentSecretNames(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{PipelinesNamespaceAuto: false}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "abcdefghijklmnopqrstuvwxyz1234567890abcd", // 40 chars
		},
	}

	name := reconciler.getResourceName(pipeline)

	expectedName := fmt.Sprintf("%s%s-%s", pipelineScopedResourcePrefix, pipeline.Spec.ID, "secret")
	if name != expectedName {
		t.Fatalf("unexpected secret name: got=%q want=%q", name, expectedName)
	}
	if got, want := len(name), len(expectedName); got != want {
		t.Fatalf("unexpected secret name length: got=%d want=%d name=%q", got, want, name)
	}
	if !strings.HasPrefix(name, pipelineScopedResourcePrefix) {
		t.Fatalf("resource name must keep prefix %q, got %q", pipelineScopedResourcePrefix, name)
	}
}

func TestPipelineScopedResourceNameAutoNamespaceUsesBaseName(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{PipelinesNamespaceAuto: true}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "very-long-pipeline-id-that-should-not-matter-here",
		},
	}

	if got, want := reconciler.getResourceName(pipeline), "secret"; got != want {
		t.Fatalf("unexpected resource name: got=%q want=%q", got, want)
	}
	if got, want := reconciler.getStatefulSetResourceName(pipeline, "ingestor-0"), "ingestor-0"; got != want {
		t.Fatalf("unexpected statefulset resource name: got=%q want=%q", got, want)
	}
}

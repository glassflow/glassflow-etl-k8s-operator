package controller

import (
	"strings"
	"testing"
)

func TestSetupWithManagerInvalidConfigReturnsError(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{
		Config: ReconcilerConfig{
			Namespaces: PipelineNamespaces{
				Auto: false,
				Name: " ",
			},
		},
	}

	err := r.SetupWithManager(nil)
	if err == nil {
		t.Fatalf("expected SetupWithManager to fail for invalid config")
	}
	if !strings.Contains(err.Error(), "validate reconciler config") {
		t.Fatalf("expected validation error context, got: %v", err)
	}
}

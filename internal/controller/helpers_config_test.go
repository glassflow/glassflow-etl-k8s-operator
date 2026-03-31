package controller

import (
	"testing"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
)

func TestGetTargetNamespaceUsesConfig(t *testing.T) {
	t.Parallel()

	p := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{ID: "abc123"},
	}

	reconcilerAuto := &PipelineReconciler{
		Config: ReconcilerConfig{
			Namespaces: PipelineNamespaces{Auto: true, Name: "ignored"},
		},
	}
	if got, want := reconcilerAuto.getTargetNamespace(p), "pipeline-abc123"; got != want {
		t.Fatalf("unexpected auto namespace: got=%q want=%q", got, want)
	}

	reconcilerStatic := &PipelineReconciler{
		Config: ReconcilerConfig{
			Namespaces: PipelineNamespaces{Auto: false, Name: "glassflow-pipelines"},
		},
	}
	if got, want := reconcilerStatic.getTargetNamespace(p), "glassflow-pipelines"; got != want {
		t.Fatalf("unexpected static namespace: got=%q want=%q", got, want)
	}
}

func TestGetUsageStatsEnvVarsUsesConfig(t *testing.T) {
	t.Parallel()

	reconcilerDisabled := &PipelineReconciler{
		Config: ReconcilerConfig{
			UsageStats: UsageStatsSettings{Enabled: false},
		},
	}
	if env := reconcilerDisabled.getUsageStatsEnvVars(); len(env) != 0 {
		t.Fatalf("expected no env vars when usage stats disabled, got %d", len(env))
	}

	reconcilerEnabled := &PipelineReconciler{
		Config: ReconcilerConfig{
			UsageStats: UsageStatsSettings{
				Enabled:        true,
				Endpoint:       "https://usage.example.com",
				Username:       "user",
				Password:       "pass",
				InstallationID: "inst-1",
			},
		},
	}
	env := reconcilerEnabled.getUsageStatsEnvVars()
	if len(env) != 5 {
		t.Fatalf("expected 5 env vars, got %d", len(env))
	}

	expected := map[string]string{
		"GLASSFLOW_USAGE_STATS_ENABLED":         "true",
		"GLASSFLOW_USAGE_STATS_ENDPOINT":        "https://usage.example.com",
		"GLASSFLOW_USAGE_STATS_USERNAME":        "user",
		"GLASSFLOW_USAGE_STATS_PASSWORD":        "pass",
		"GLASSFLOW_USAGE_STATS_INSTALLATION_ID": "inst-1",
	}
	for _, item := range env {
		want, ok := expected[item.Name]
		if !ok {
			t.Fatalf("unexpected env var: %s", item.Name)
		}
		if item.Value != want {
			t.Fatalf("unexpected env var value for %s: got=%q want=%q", item.Name, item.Value, want)
		}
		delete(expected, item.Name)
	}
	if len(expected) != 0 {
		t.Fatalf("missing expected env vars: %v", expected)
	}
}

func TestGetComponentDatabaseEnvVarsUsesConfig(t *testing.T) {
	t.Parallel()

	reconcilerEmpty := &PipelineReconciler{}
	if env := reconcilerEmpty.getComponentDatabaseEnvVars(); env != nil {
		t.Fatalf("expected nil env vars when database URL not configured")
	}

	reconciler := &PipelineReconciler{
		Config: ReconcilerConfig{
			DatabaseURL: "postgres://user:pass@host/db",
		},
	}
	env := reconciler.getComponentDatabaseEnvVars()
	if len(env) != 1 {
		t.Fatalf("expected 1 env var, got %d", len(env))
	}
	item := env[0]
	if item.Name != "GLASSFLOW_DATABASE_URL" {
		t.Fatalf("unexpected env var name: %s", item.Name)
	}
	if item.ValueFrom == nil || item.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("expected secret key ref for database env var")
	}
	if item.ValueFrom.SecretKeyRef.Name != constants.ComponentDatabaseSecretName {
		t.Fatalf("unexpected secret name: %s", item.ValueFrom.SecretKeyRef.Name)
	}
	if item.ValueFrom.SecretKeyRef.Key != constants.ComponentDatabaseSecretKey {
		t.Fatalf("unexpected secret key: %s", item.ValueFrom.SecretKeyRef.Key)
	}
}

func TestGetComponentEncryptionHelpersUseConfig(t *testing.T) {
	t.Parallel()

	reconcilerDisabled := &PipelineReconciler{}
	if _, ok := reconcilerDisabled.getComponentEncryptionVolume(); ok {
		t.Fatalf("expected no encryption volume when encryption disabled")
	}
	if _, ok := reconcilerDisabled.getComponentEncryptionVolumeMount(); ok {
		t.Fatalf("expected no encryption volume mount when encryption disabled")
	}

	reconcilerEnabled := &PipelineReconciler{
		Config: ReconcilerConfig{
			Encryption: EncryptionSettings{Enabled: true},
		},
	}
	vol, ok := reconcilerEnabled.getComponentEncryptionVolume()
	if !ok {
		t.Fatalf("expected encryption volume when encryption enabled")
	}
	if vol.Name != "encryption-key" {
		t.Fatalf("unexpected encryption volume name: %s", vol.Name)
	}
	if vol.Secret == nil {
		t.Fatalf("expected secret volume source")
	}
	if vol.Secret.SecretName != constants.ComponentEncryptionSecretName {
		t.Fatalf("unexpected encryption secret name: %s", vol.Secret.SecretName)
	}

	mount, ok := reconcilerEnabled.getComponentEncryptionVolumeMount()
	if !ok {
		t.Fatalf("expected encryption volume mount when encryption enabled")
	}
	if mount.Name != "encryption-key" {
		t.Fatalf("unexpected mount name: %s", mount.Name)
	}
	if mount.MountPath != "/etc/glassflow/secrets" {
		t.Fatalf("unexpected mount path: %s", mount.MountPath)
	}
	if !mount.ReadOnly {
		t.Fatalf("expected read-only encryption mount")
	}
}

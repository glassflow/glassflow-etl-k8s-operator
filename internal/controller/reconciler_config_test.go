package controller

import "testing"

func TestReconcilerConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ReconcilerConfig
		wantErr bool
	}{
		{
			name: "auto namespace enabled allows empty name",
			cfg: ReconcilerConfig{
				Namespaces: PipelineNamespaces{Auto: true, Name: ""},
			},
			wantErr: false,
		},
		{
			name: "auto namespace disabled with explicit name is valid",
			cfg: ReconcilerConfig{
				Namespaces: PipelineNamespaces{Auto: false, Name: "glassflow-pipelines"},
			},
			wantErr: false,
		},
		{
			name: "auto namespace disabled requires name",
			cfg: ReconcilerConfig{
				Namespaces: PipelineNamespaces{Auto: false, Name: ""},
			},
			wantErr: true,
		},
		{
			name: "auto namespace disabled rejects whitespace-only name",
			cfg: ReconcilerConfig{
				Namespaces: PipelineNamespaces{Auto: false, Name: "   "},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no validation error, got %v", err)
			}
		})
	}
}

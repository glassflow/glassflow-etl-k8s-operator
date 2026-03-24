package notifications

import (
	"context"
	"errors"
	"testing"
)

type testClient struct {
	enabled      bool
	publishErr   error
	publishedCnt int
}

func (c *testClient) Publish(_ context.Context, _ *Notification) error {
	c.publishedCnt++
	return c.publishErr
}

func (c *testClient) IsEnabled() bool {
	return c.enabled
}

func (c *testClient) Close() error {
	return nil
}

func setRuntimeClientForTest(client Client) func() {
	runtimeMu.Lock()
	oldClient := runtimeClient
	runtimeClient = client
	runtimeMu.Unlock()

	return func() {
		runtimeMu.Lock()
		runtimeClient = oldClient
		runtimeMu.Unlock()
	}
}

func TestPublishUsesRuntimeClient(t *testing.T) {
	stub := &testClient{enabled: true}
	restore := setRuntimeClientForTest(stub)
	defer restore()

	notification := NewPipelineDeployedNotification("pipeline-1", "", "", nil)
	if err := Publish(context.Background(), notification); err != nil {
		t.Fatalf("Publish() returned error: %v", err)
	}
	if stub.publishedCnt != 1 {
		t.Fatalf("published count = %d, want 1", stub.publishedCnt)
	}
}

func TestPublishReturnsClientError(t *testing.T) {
	stub := &testClient{enabled: true, publishErr: errors.New("boom")}
	restore := setRuntimeClientForTest(stub)
	defer restore()

	notification := NewPipelineDeployedNotification("pipeline-1", "", "", nil)
	err := Publish(context.Background(), notification)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestPublishIgnoresDisabledError(t *testing.T) {
	stub := &testClient{enabled: false, publishErr: ErrNotificationDisabled}
	restore := setRuntimeClientForTest(stub)
	defer restore()

	notification := NewPipelineDeployedNotification("pipeline-1", "", "", nil)
	if err := Publish(context.Background(), notification); err != nil {
		t.Fatalf("Publish() returned error: %v", err)
	}
}

func TestInitializeDisabledWithoutNATS(t *testing.T) {
	cfg := DefaultConfig().WithEnabled(false)
	if err := Initialize(context.Background(), cfg, nil, runtimeLogger); err != nil {
		t.Fatalf("Initialize() returned error: %v", err)
	}
}

func TestInitializeEnabledRequiresNATS(t *testing.T) {
	cfg := DefaultConfig().WithEnabled(true)
	err := Initialize(context.Background(), cfg, nil, runtimeLogger)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

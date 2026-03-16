package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/glassflow/glassflow-etl-k8s-operator/pkg/usagestats"
	"github.com/go-logr/logr"
)

func TestRecordReconcileErrorWithNilUsageStatsClient(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	r.recordReconcileError(context.Background(), "create", "pipeline-1", errors.New("boom"))
}

func TestSendReconcileSuccessEventWithNilUsageStatsClient(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	r.sendReconcileSuccessEvent(context.Background(), "create", "pipeline-1")
}

func TestSendUsageStatsEventWithNilUsageStatsClient(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{}
	r.sendUsageStatsEvent(context.Background(), "event", map[string]interface{}{"key": "value"})
}

func TestSendUsageStatsEventWithDisabledUsageStatsClient(t *testing.T) {
	t.Parallel()

	r := &PipelineReconciler{
		UsageStatsClient: usagestats.NewClient("", "", "", "", false, logr.Discard()),
	}
	r.sendUsageStatsEvent(context.Background(), "event", map[string]interface{}{"key": "value"})
}

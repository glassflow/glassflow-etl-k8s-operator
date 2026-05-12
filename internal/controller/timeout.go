/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/pkg/usagestats"
)

func getPipelineOperationFromAnnotations(annotations map[string]string) string {
	if annotations == nil {
		return ""
	}

	// avoid switch to ensure priority for annotations / operations
	if _, exists := annotations[constants.PipelineHelmUninstallAnnotation]; exists {
		return constants.OperationHelmUninstall
	}
	if _, exists := annotations[constants.PipelineDeleteAnnotation]; exists {
		return constants.OperationDelete
	}
	if _, exists := annotations[constants.PipelineTerminateAnnotation]; exists {
		return constants.OperationTerminate
	}
	if _, exists := annotations[constants.PipelineCreateAnnotation]; exists {
		return constants.OperationCreate
	}
	if _, exists := annotations[constants.PipelineStopAnnotation]; exists {
		return constants.OperationStop
	}
	if _, exists := annotations[constants.PipelineResumeAnnotation]; exists {
		return constants.OperationResume
	}
	if _, exists := annotations[constants.PipelineEditAnnotation]; exists {
		return constants.OperationEdit
	}

	return ""
}

func clearOperationAnnotation(annotations map[string]string, operation string) {
	switch operation {
	case constants.OperationCreate:
		delete(annotations, constants.PipelineCreateAnnotation)
	case constants.OperationResume:
		delete(annotations, constants.PipelineResumeAnnotation)
	case constants.OperationStop:
		delete(annotations, constants.PipelineStopAnnotation)
	case constants.OperationEdit:
		delete(annotations, constants.PipelineEditAnnotation)
	case constants.OperationTerminate:
		delete(annotations, constants.PipelineTerminateAnnotation)
	case constants.OperationDelete:
		delete(annotations, constants.PipelineDeleteAnnotation)
	case constants.OperationHelmUninstall:
		delete(annotations, constants.PipelineHelmUninstallAnnotation)
	}
}

// operationTimeoutFor returns the active timeout for the given operation. Stop uses a
// shorter ceiling than the other operations because "stop" is intent-clear and shouldn't
// wait as long as create/edit, which need minutes for StatefulSet rollouts.
func operationTimeoutFor(annotations map[string]string) time.Duration {
	if getPipelineOperationFromAnnotations(annotations) == constants.OperationStop {
		return constants.StopReconcileTimeout
	}
	return constants.ReconcileTimeout
}

// checkOperationTimeout checks if an operation has exceeded the timeout duration
// Returns true if timed out, false otherwise, and the elapsed duration
func (r *PipelineReconciler) checkOperationTimeout(log logr.Logger, p *etlv1alpha1.Pipeline) (bool, time.Duration) {
	annotations := p.GetAnnotations()
	if annotations == nil {
		return false, 0
	}

	startTimeStr, exists := annotations[constants.PipelineOperationStartTimeAnnotation]
	if !exists {
		return false, 0
	}

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		log.Error(err, "failed to parse operation start time", "pipeline_id", p.Spec.ID, "start_time", startTimeStr)
		// If we can't parse the time, clear it and continue
		r.clearOperationStartTime(p)
		return false, 0
	}

	timeout := operationTimeoutFor(annotations)
	elapsed := time.Since(startTime)
	if elapsed > timeout {
		log.Info("operation timed out", "pipeline_id", p.Spec.ID, "elapsed", elapsed, "timeout", timeout)
		return true, elapsed
	}

	return false, elapsed
}

// setOperationStartTime sets the operation start time annotation if not already set
func (r *PipelineReconciler) setOperationStartTime(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Only set if not already set (to preserve the original start time)
	if _, exists := annotations[constants.PipelineOperationStartTimeAnnotation]; !exists {
		annotations[constants.PipelineOperationStartTimeAnnotation] = time.Now().UTC().Format(time.RFC3339)
		p.SetAnnotations(annotations)
		return r.Update(ctx, p)
	}

	return nil
}

// clearOperationStartTime clears the operation start time annotation
func (r *PipelineReconciler) clearOperationStartTime(p *etlv1alpha1.Pipeline) {
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineOperationStartTimeAnnotation)
		p.SetAnnotations(annotations)
		// Note: We don't update here, caller should handle the update
	}
}

// handleOperationTimeout handles a timed-out operation. Non-stop operations terminate
// components and end in Failed. Stop is special: after force-terminating components we
// also sweep orphaned messages to the DLQ and clean up internal NATS streams, then end
// in Stopped — so a user's stop intent is honoured even when consumers couldn't drain.
// Sweep or cleanup failures keep the pipeline in Failed so the incident remains visible.
func (r *PipelineReconciler) handleOperationTimeout(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID
	operation := getPipelineOperationFromAnnotations(p.GetAnnotations())
	if operation == "" {
		log.Info("could not determine operation from pipeline annotations while handling timeout", "pipeline_id", pipelineID)
		operation = "unknown"
	}

	timeout := operationTimeoutFor(p.GetAnnotations())
	log.Error(fmt.Errorf("operation timed out after %v", timeout), "operation timed out", "pipeline_id", pipelineID, "operation", operation)

	errorMsg := fmt.Sprintf("operation timed out after %v", timeout)
	// For stop we defer the terminal status until we know whether the forced sweep + cleanup
	// succeed, to avoid briefly publishing Failed before flipping back to Stopped.
	if operation != constants.OperationStop {
		if err := r.updatePipelineStatus(ctx, log, p, models.PipelineStatusFailed, []string{errorMsg}, "timeout"); err != nil {
			log.Error(err, "failed to update pipeline status to Failed", "pipeline_id", pipelineID)
			// Continue anyway to clear annotations
		}
	}

	// Clear operation start time
	r.clearOperationStartTime(p)

	// Clear the operation annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		clearOperationAnnotation(annotations, operation)
		p.SetAnnotations(annotations)

		// Terminate all pipeline components
		result, err := r.terminatePipelineComponents(ctx, log, p)
		if err != nil || result.Requeue {
			return result, err
		}

		finalStatus := models.PipelineStatusFailed
		var stopErrors []string
		if operation == constants.OperationStop {
			if sweepErr := r.sweepMessagesToDLQ(ctx, log, *p); sweepErr != nil {
				log.Error(sweepErr, "forced stop: sweep orphaned messages failed", "pipeline_id", pipelineID)
				stopErrors = []string{fmt.Sprintf("forced stop sweep failed: %v", sweepErr)}
			} else if cleanupErr := r.cleanupNATSPipelineResourcesKeepDLQ(ctx, log, *p); cleanupErr != nil {
				log.Error(cleanupErr, "forced stop: cleanup NATS resources failed", "pipeline_id", pipelineID)
				stopErrors = []string{fmt.Sprintf("forced stop cleanup failed: %v", cleanupErr)}
			} else {
				finalStatus = models.PipelineStatusStopped
				r.clearStopLastPendingCount(p)
			}
		}

		// Persist annotation clears (and the in-memory status flip) before pushing the
		// terminal status through updatePipelineStatus. updatePipelineStatus uses the
		// Status subresource, which intentionally does not carry metadata changes —
		// so the metadata Update must land first to avoid losing the annotation clears.
		p.Status = etlv1alpha1.PipelineStatus(finalStatus)
		if err := r.Update(ctx, p); err != nil {
			log.Error(err, "failed to clear operation annotation after timeout", "pipeline_id", pipelineID)
		}

		if operation == constants.OperationStop {
			if err := r.updatePipelineStatus(ctx, log, p, finalStatus, stopErrors, "timeout"); err != nil {
				log.Error(err, "failed to update pipeline final status after forced stop", "pipeline_id", pipelineID)
			}
		}
	}

	// Record timeout metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, operation, "timeout", pipelineID)
	})

	// Send usageStat event for reconcile timeout
	r.sendUsageStatsEvent(ctx, "reconcile_timeout", map[string]interface{}{
		"pipeline_id_hash": usagestats.HashPipelineID(pipelineID),
		"operation":        operation,
		"status":           "timeout",
		"error":            errorMsg,
		"cluster_provider": r.Config.ClusterProvider,
	})
	r.sendOperationFailureNotification(ctx, operation, pipelineID, fmt.Errorf("%s", errorMsg))

	return ctrl.Result{}, nil // Don't requeue - operation has timed out
}

// extendOperationTimeout resets the operation start time to now, effectively restarting the timeout window.
func (r *PipelineReconciler) extendOperationTimeout(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[constants.PipelineOperationStartTimeAnnotation] = time.Now().UTC().Format(time.RFC3339)
	p.SetAnnotations(annotations)
	return r.Update(ctx, p)
}

// setStopLastPendingCount stores the current total pending message count in an annotation.
func (r *PipelineReconciler) setStopLastPendingCount(ctx context.Context, p *etlv1alpha1.Pipeline, count int) error {
	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[constants.PipelineStopLastPendingCountAnnotation] = strconv.Itoa(count)
	p.SetAnnotations(annotations)
	return r.Update(ctx, p)
}

// getStopLastPendingCount reads the last recorded pending count annotation.
// Returns (count, true) if the annotation is set and valid, or (0, false) otherwise.
func getStopLastPendingCount(p *etlv1alpha1.Pipeline) (int, bool) {
	annotations := p.GetAnnotations()
	if annotations == nil {
		return 0, false
	}
	str, exists := annotations[constants.PipelineStopLastPendingCountAnnotation]
	if !exists {
		return 0, false
	}
	count, err := strconv.Atoi(str)
	if err != nil {
		return 0, false
	}
	return count, true
}

// clearStopLastPendingCount removes the last-pending-count annotation from the pipeline.
func (r *PipelineReconciler) clearStopLastPendingCount(p *etlv1alpha1.Pipeline) {
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineStopLastPendingCountAnnotation)
		p.SetAnnotations(annotations)
	}
}

// setOTLPDownscaleSubjectCount persists the old OTLP source stream count in an annotation
// so it survives reconcile passes after stale streams are deleted. Idempotent — only stores
// on the first call (to preserve the original count).
func (r *PipelineReconciler) setOTLPDownscaleSubjectCount(ctx context.Context, p *etlv1alpha1.Pipeline, count int) error {
	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if _, exists := annotations[constants.PipelineOTLPDownscaleOldSubjectCountAnnotation]; exists {
		return nil
	}
	annotations[constants.PipelineOTLPDownscaleOldSubjectCountAnnotation] = strconv.Itoa(count)
	p.SetAnnotations(annotations)
	return r.Update(ctx, p)
}

// getOTLPDownscaleSubjectCount reads the persisted old OTLP subject count annotation.
// Returns (count, true) if valid, (0, false) otherwise.
func getOTLPDownscaleSubjectCount(p *etlv1alpha1.Pipeline) (int, bool) {
	annotations := p.GetAnnotations()
	if annotations == nil {
		return 0, false
	}
	str, exists := annotations[constants.PipelineOTLPDownscaleOldSubjectCountAnnotation]
	if !exists {
		return 0, false
	}
	count, err := strconv.Atoi(str)
	if err != nil || count == 0 {
		return 0, false
	}
	return count, true
}

// clearOTLPDownscaleSubjectCount removes the old subject count annotation from the pipeline.
// The caller is responsible for persisting the change via Update.
func (r *PipelineReconciler) clearOTLPDownscaleSubjectCount(p *etlv1alpha1.Pipeline) {
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineOTLPDownscaleOldSubjectCountAnnotation)
		p.SetAnnotations(annotations)
	}
}

// tryExtendEditDrainTimeout extends the edit operation timeout when NATS Stream Source transfer
// is still making progress. It sums message counts across stale streams and compares against the
// last recorded value; returns true (extend) if decreasing, false (give up) if stalled.
func (r *PipelineReconciler) tryExtendEditDrainTimeout(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	staleNames []string,
) (bool, error) {
	var current uint64
	for _, name := range staleNames {
		count, err := r.NATSClient.GetStreamMessageCount(ctx, name)
		if err != nil {
			return false, fmt.Errorf("get message count for stale stream %s: %w", name, err)
		}
		current += count
	}

	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	lastStr := annotations[constants.PipelineEditDrainLastMsgCountAnnotation]
	lastTotal, _ := strconv.ParseUint(lastStr, 10, 64)
	if lastStr != "" && current >= lastTotal {
		log.Info("stale stream drain stalled, honoring edit timeout", "pipeline_id", p.Spec.ID, "total_msgs", current)
		return false, nil
	}

	log.Info("stale stream drain in progress, extending edit timeout", "pipeline_id", p.Spec.ID, "total_msgs", current)
	annotations[constants.PipelineEditDrainLastMsgCountAnnotation] = strconv.FormatUint(current, 10)
	annotations[constants.PipelineOperationStartTimeAnnotation] = time.Now().UTC().Format(time.RFC3339)
	p.SetAnnotations(annotations)
	if err := r.Update(ctx, p); err != nil {
		return false, fmt.Errorf("extend edit drain timeout: %w", err)
	}
	return true, nil
}

// tryExtendStopTimeout checks whether pending messages are still decreasing when a stop operation
// times out. If the count has decreased since the last check, it extends the timeout window and
// returns true so the caller can requeue. If no progress is detected, it returns false and the
// caller should proceed with the normal timeout failure path.
func (r *PipelineReconciler) tryExtendStopTimeout(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (bool, error) {
	pipelineID := p.Spec.ID

	currentCount, err := r.getTotalPendingCount(ctx, *p)
	if err != nil {
		return false, fmt.Errorf("get total pending count: %w", err)
	}

	lastCount, hasLastCount := getStopLastPendingCount(p)
	log.Info("checking stop timeout extension",
		"pipeline_id", pipelineID,
		"current_pending", currentCount,
		"last_pending", lastCount,
		"has_last_count", hasLastCount,
	)

	// Extend the timeout if this is the first check or if messages are being consumed.
	if !hasLastCount || currentCount < lastCount {
		log.Info("pending messages are decreasing, extending stop timeout",
			"pipeline_id", pipelineID,
			"current_pending", currentCount,
			"last_pending", lastCount,
		)

		annotations := p.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[constants.PipelineStopLastPendingCountAnnotation] = strconv.Itoa(currentCount)
		annotations[constants.PipelineOperationStartTimeAnnotation] = time.Now().UTC().Format(time.RFC3339)
		p.SetAnnotations(annotations)
		if err := r.Update(ctx, p); err != nil {
			return false, fmt.Errorf("extend stop timeout and save pending count: %w", err)
		}
		return true, nil
	}

	log.Info("pending messages not decreasing, honoring stop timeout",
		"pipeline_id", pipelineID,
		"current_pending", currentCount,
		"last_pending", lastCount,
	)
	return false, nil
}

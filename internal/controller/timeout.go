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
	"time"

	postgresstorage "github.com/glassflow/glassflow-etl-k8s-operator/internal/storage/postgres"
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/pkg/tracking"
)

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

	elapsed := time.Since(startTime)
	if elapsed > constants.ReconcileTimeout {
		log.Info("operation timed out", "pipeline_id", p.Spec.ID, "elapsed", elapsed, "timeout", constants.ReconcileTimeout)
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

// handleOperationTimeout handles a timed-out operation by updating status to Failed and clearing annotations
func (r *PipelineReconciler) handleOperationTimeout(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, operation string) (ctrl.Result, error) {
	pipelineID := p.Spec.ID
	log.Error(fmt.Errorf("operation timed out after %v", constants.ReconcileTimeout), "operation timed out", "pipeline_id", pipelineID, "operation", operation)

	// Update status to Failed with error message
	errorMsg := fmt.Sprintf("operation timed out after %v", constants.ReconcileTimeout)
	err := r.updatePipelineStatus(ctx, log, p, postgresstorage.PipelineStatusFailed, []string{errorMsg})
	if err != nil {
		log.Error(err, "failed to update pipeline status to Failed", "pipeline_id", pipelineID)
		// Continue anyway to clear annotations
	}

	// Clear operation start time
	r.clearOperationStartTime(p)

	// Clear the operation annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		switch operation {
		case constants.OperationCreate:
			delete(annotations, constants.PipelineCreateAnnotation)
		case constants.OperationResume:
			delete(annotations, constants.PipelineResumeAnnotation)
		case constants.OperationStop:
			delete(annotations, constants.PipelineStopAnnotation)
		case constants.OperationEdit:
			delete(annotations, constants.PipelineEditAnnotation)
		}
		p.SetAnnotations(annotations)

		// Terminate all pipeline components
		result, err := r.terminatePipelineComponents(ctx, log, *p)
		if err != nil || result.Requeue {
			return result, err
		}

		p.Status = etlv1alpha1.PipelineStatus(postgresstorage.PipelineStatusFailed)
		err = r.Update(ctx, p)
		if err != nil {
			log.Error(err, "failed to clear operation annotation after timeout", "pipeline_id", pipelineID)
		}
	}

	// Record timeout metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, operation, "timeout", pipelineID)
	})

	// Send tracking event for reconcile timeout
	r.TrackingClient.SendEvent(ctx, "reconcile_timeout", "operator", map[string]interface{}{
		"pipeline_id_hash": tracking.HashPipelineID(pipelineID),
		"operation":        operation,
		"status":           "timeout",
		"error":            errorMsg,
		"cluster_provider": r.ClusterProvider,
	})

	return ctrl.Result{}, nil // Don't requeue - operation has timed out
}

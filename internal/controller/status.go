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

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	postgresstorage "github.com/glassflow/glassflow-etl-k8s-operator/internal/storage/postgres"
	"github.com/glassflow/glassflow-etl-k8s-operator/pkg/usagestats"
)

// recordReconcileError records error metrics and sends usage stats event for reconcile operations
func (r *PipelineReconciler) recordReconcileError(ctx context.Context, operation, pipelineID string, err error) {
	if r.Meter != nil {
		r.Meter.RecordReconcileOperation(ctx, operation, "failure", pipelineID)
		r.Meter.RecordReconcileError(ctx, operation, err.Error(), pipelineID)
	}

	// Send usage stats event for reconcile failure
	r.UsageStatsClient.SendEvent(ctx, "reconcile_error", "operator", map[string]interface{}{
		"pipeline_id_hash": usagestats.HashPipelineID(pipelineID),
		"operation":        operation,
		"status":           "failure",
		"error":            err.Error(),
		"cluster_provider": r.ClusterProvider,
	})
}

// sendReconcileSuccessEvent sends a usage stats event for successful reconcile operations
func (r *PipelineReconciler) sendReconcileSuccessEvent(ctx context.Context, operation, pipelineID string) {
	r.UsageStatsClient.SendEvent(ctx, "reconcile_success", "operator", map[string]interface{}{
		"pipeline_id_hash": usagestats.HashPipelineID(pipelineID),
		"operation":        operation,
		"status":           "success",
		"cluster_provider": r.ClusterProvider,
	})
}

// recordMetricsIfEnabled is a helper function to safely record metrics only if the meter is available
func (r *PipelineReconciler) recordMetricsIfEnabled(fn func(*observability.Meter)) {
	if r.Meter != nil {
		fn(r.Meter)
	}
}

// updatePipelineStatus updates PostgreSQL and CRD status (validation handled by backend API)
func (r *PipelineReconciler) updatePipelineStatus(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, newStatus postgresstorage.PipelineStatus, errors []string) error {
	// Check if status is already the same - avoid duplicate updates and history entries
	currentStatus := postgresstorage.PipelineStatus(p.Status)
	if currentStatus == newStatus {
		log.V(1).Info("pipeline status unchanged, skipping update", "pipeline_id", p.Spec.ID, "status", newStatus)
		return nil
	}

	// Status validation is now handled by the backend API, so we trust the status update

	// Update PostgreSQL storage
	if r.PostgresStorage != nil {
		pgStatus := newStatus
		err := r.PostgresStorage.UpdatePipelineStatus(ctx, p.Spec.ID, pgStatus, errors)
		if err != nil {
			log.Info("failed to update pipeline status in PostgreSQL", "pipeline_id", p.Spec.ID, "status", newStatus, "error", err)
			// Don't fail the reconciliation if PostgreSQL update fails, just log the error
		} else {
			log.Info("successfully updated pipeline status in PostgreSQL", "pipeline_id", p.Spec.ID, "status", newStatus)
		}
	}

	// Update CRD status
	oldStatus := string(p.Status)
	p.Status = etlv1alpha1.PipelineStatus(newStatus)
	err := r.Status().Update(ctx, p, &client.SubResourceUpdateOptions{})
	if err != nil {
		return fmt.Errorf("update pipeline CRD status: %w", err)
	}

	// Record status transition metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordStatusTransition(ctx, oldStatus, string(newStatus), p.Spec.ID)
	})

	// Send usage stats event for status change
	r.UsageStatsClient.SendEvent(ctx, "pipeline_status_change", "operator", map[string]interface{}{
		"pipeline_id_hash": usagestats.HashPipelineID(p.Spec.ID),
		"status":           string(newStatus),
		"cluster_provider": r.ClusterProvider,
	})

	log.Info("pipeline status updated successfully", "pipeline_id", p.Spec.ID, "to", newStatus)
	return nil
}

// addFinalizer adds a finalizer to the pipeline
func (r *PipelineReconciler) addFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if !containsFinalizer(p.Finalizers, constants.PipelineFinalizerName) {
		p.Finalizers = append(p.Finalizers, constants.PipelineFinalizerName)
		err := r.Update(ctx, p)
		if err != nil {
			return fmt.Errorf("add finalizer: %w", err)
		}
	}
	return nil
}

// removeFinalizer removes a finalizer from the pipeline
func (r *PipelineReconciler) removeFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if containsFinalizer(p.Finalizers, constants.PipelineFinalizerName) {
		// Remove the finalizer from the slice
		finalizers := make([]string, 0, len(p.Finalizers)-1)
		for _, f := range p.Finalizers {
			if f != constants.PipelineFinalizerName {
				finalizers = append(finalizers, f)
			}
		}
		p.Finalizers = finalizers

		err := r.Update(ctx, p)
		if err != nil {
			return fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return nil
}

// containsFinalizer checks if a finalizer exists in the finalizers slice
func containsFinalizer(finalizers []string, finalizer string) bool {
	for _, f := range finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

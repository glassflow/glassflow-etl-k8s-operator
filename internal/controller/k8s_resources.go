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
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
)

// createNamespace creates or gets a namespace for the pipeline
func (r *PipelineReconciler) createNamespace(ctx context.Context, p etlv1alpha1.Pipeline) (zero v1.Namespace, _ error) {
	targetNamespace := r.getTargetNamespace(p)

	// If auto=false, we don't create namespaces, just return the target namespace
	if !r.PipelinesNamespaceAuto {
		var ns v1.Namespace
		err := r.Get(ctx, types.NamespacedName{Name: targetNamespace}, &ns)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return zero, fmt.Errorf("target namespace %s does not exist and auto=false", targetNamespace)
			}
			return zero, fmt.Errorf("get namespace %s: %w", targetNamespace, err)
		}
		return ns, nil
	}

	// Auto=true: create per-pipeline namespace
	var ns v1.Namespace
	err := r.Get(ctx, types.NamespacedName{Name: targetNamespace}, &ns)
	if err == nil {
		return ns, nil
	}

	if !apierrors.IsNotFound(err) {
		return zero, fmt.Errorf("get namespace %s: %w", targetNamespace, err)
	}

	ns = v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetNamespace,
			Annotations: map[string]string{
				"etl.glassflow.io/managed-by": "glassflow-operator",
				"etl.glassflow.io/mode":       "auto",
			},
		},
	}

	err = r.Create(ctx, &ns, &client.CreateOptions{})
	if err != nil {
		return zero, fmt.Errorf("create namespace %s: %w", ns.GetName(), err)
	}

	return ns, nil
}

// deleteNamespace deletes a namespace for the pipeline
func (r *PipelineReconciler) deleteNamespace(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	targetNamespace := r.getTargetNamespace(p)
	log.Info("deleting pipeline namespace", "pipeline", p.Name, "pipeline_id", p.Spec.ID, "namespace", targetNamespace)

	// If auto=false, we don't delete namespaces, just clean up resources
	if !r.PipelinesNamespaceAuto {
		log.Info("auto=false, skipping namespace deletion", "namespace", targetNamespace)
		return nil
	}

	namespaceName := types.NamespacedName{Name: targetNamespace}

	var namespace v1.Namespace
	err := r.Get(ctx, namespaceName, &namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("namespace already deleted", "namespace", namespaceName.Name)
			return nil
		}
		return fmt.Errorf("get namespace %s: %w", namespaceName.Name, err)
	}

	// Safety check: only delete operator-managed namespaces
	if !r.isOperatorManagedNamespace(namespace) {
		log.Info("namespace not managed by operator, skipping deletion", "namespace", namespaceName.Name)
		return nil
	}

	err = r.Delete(ctx, &namespace, &client.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete namespace %s: %w", namespaceName.Name, err)
	}

	log.Info("namespace deleted successfully", "namespace", namespaceName.Name)
	return nil
}

// createDeployment creates a deployment
func (r *PipelineReconciler) createDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	err := r.Create(ctx, deployment, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}

		return fmt.Errorf("create deployment: %w", err)
	}

	return nil
}

// createSecret creates a secret for pipeline configuration
func (r *PipelineReconciler) createSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels:    labels,
		},
		Immutable: ptrBool(true),
		StringData: map[string]string{
			"pipeline.json": p.Spec.Config,
		},
		Type: v1.SecretTypeOpaque,
	}
	err := r.Create(ctx, &s, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return s, nil
		}
		return zero, fmt.Errorf("create secret %s: %w", namespacedName, err)
	}

	return s, nil
}

// updateSecret updates a secret by deleting and recreating it (since secrets are immutable)
func (r *PipelineReconciler) updateSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	// First, try to get the existing secret
	var existingSecret v1.Secret
	err := r.Get(ctx, namespacedName, &existingSecret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Secret doesn't exist, create it
			return r.createSecret(ctx, namespacedName, labels, p)
		}
		return zero, fmt.Errorf("get existing secret %s: %w", namespacedName, err)
	}

	// Delete the existing secret since it's immutable and cannot be updated
	// We need to recreate it with the new configuration
	err = r.Delete(ctx, &existingSecret)
	if err != nil {
		return zero, fmt.Errorf("delete existing secret %s: %w", namespacedName, err)
	}

	// Create a new secret with the updated config
	return r.createSecret(ctx, namespacedName, labels, p)
}

// isDeploymentAbsent checks if a deployment is fully deleted
func (r *PipelineReconciler) isDeploymentAbsent(ctx context.Context, namespace, name string) (bool, error) {
	var deployment appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get deployment %s: %w", name, err)
	}
	return false, nil
}

// isDeploymentReady checks if a deployment is ready
func (r *PipelineReconciler) isDeploymentReady(ctx context.Context, namespace, name string) (bool, error) {
	var deployment appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get deployment %s: %w", name, err)
	}

	// Check if deployment is ready
	if deployment.Status.ReadyReplicas == deployment.Status.Replicas && deployment.Status.Replicas > 0 {
		return true, nil
	}
	return false, nil
}

// deleteDeployment safely deletes a deployment, handling NotFound errors gracefully
func (r *PipelineReconciler) deleteDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	err := r.Delete(ctx, deployment, &client.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Already deleted, that's fine
		}
		return fmt.Errorf("delete deployment: %w", err)
	}
	return nil
}

// -------------------------------------------------------------------------------------------------------------------
// StatefulSet Operations
// -------------------------------------------------------------------------------------------------------------------

// createStatefulSet creates a StatefulSet
func (r *PipelineReconciler) createStatefulSet(ctx context.Context, sts *appsv1.StatefulSet) error {
	err := r.Create(ctx, sts)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create statefulset: %w", err)
	}
	return nil
}

// isStatefulSetReady checks if a StatefulSet is ready
func (r *PipelineReconciler) isStatefulSetReady(ctx context.Context, namespace, name string) (bool, error) {
	var sts appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	if sts.Spec.Replicas == nil {
		return false, nil
	}

	return sts.Status.ReadyReplicas == *sts.Spec.Replicas && *sts.Spec.Replicas > 0, nil
}

// isStatefulSetAbsent checks if a StatefulSet is absent
func (r *PipelineReconciler) isStatefulSetAbsent(ctx context.Context, namespace, name string) (bool, error) {
	var sts appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// deleteStatefulSet deletes a StatefulSet
func (r *PipelineReconciler) deleteStatefulSet(ctx context.Context, sts *appsv1.StatefulSet) error {
	err := r.Delete(ctx, sts)
	if err != nil {
		return fmt.Errorf("delete statefulset: %w", err)
	}
	return nil
}

// cleanupDedupPVCs deletes PVCs associated with dedup StatefulSets
func (r *PipelineReconciler) cleanupDedupPVCs(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	namespace := r.getTargetNamespace(p)

	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(p, fmt.Sprintf("dedup-%d", i))

		replicas := 1

		// StatefulSet PVCs have format: data-{statefulset-name}-{ordinal}
		for replica := 0; replica < replicas; replica++ {
			pvcName := fmt.Sprintf("data-%s-%d", dedupName, replica)

			var pvc v1.PersistentVolumeClaim
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, &pvc)
			if err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return fmt.Errorf("get pvc %s: %w", pvcName, err)
			}

			log.Info("deleting dedup PVC", "pvc", pvcName, "namespace", namespace)
			err = r.Delete(ctx, &pvc)
			if err != nil {
				return fmt.Errorf("delete pvc %s: %w", pvcName, err)
			}
		}
	}

	return nil
}

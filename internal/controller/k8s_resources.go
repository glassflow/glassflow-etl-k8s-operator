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
	"errors"
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

var ErrPipelineConfigSecretNotFound = errors.New("pipeline config secret not found")

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

// getPipelineConfigFromSecret reads the pipeline configuration from the secret in glassflow namespace
func (r *PipelineReconciler) getPipelineConfigFromSecret(ctx context.Context, pipelineID string) (string, error) {
	secretName := fmt.Sprintf("pipeline-config-%s", pipelineID)
	secretNamespacedName := types.NamespacedName{
		Name:      secretName,
		Namespace: r.GlassflowNamespace,
	}

	var secret v1.Secret
	err := r.Get(ctx, secretNamespacedName, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: %s in namespace %s (may need to wait for API to create it)", ErrPipelineConfigSecretNotFound, secretName, r.GlassflowNamespace)
		}
		return "", fmt.Errorf("get pipeline config secret %s: %w", secretNamespacedName, err)
	}

	// Extract pipeline.json from the secret
	pipelineJSON, exists := secret.Data["pipeline.json"]
	if !exists {
		return "", fmt.Errorf("pipeline.json not found in secret %s", secretName)
	}

	return string(pipelineJSON), nil
}

// createSecret creates a secret for pipeline configuration
func (r *PipelineReconciler) createSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	// Check if a secret already exists
	var existingSecret v1.Secret
	err := r.Get(ctx, namespacedName, &existingSecret)
	if err == nil {
		return existingSecret, nil
	}
	if !apierrors.IsNotFound(err) {
		return zero, fmt.Errorf("get existing secret %s: %w", namespacedName, err)
	}

	var pipelineConfig string
	// Read from secret created by Glassflow API
	pipelineConfig, err = r.getPipelineConfigFromSecret(ctx, p.Spec.ID)
	if err != nil {
		if p.Spec.Config != "" {
			pipelineConfig = p.Spec.Config
		} else {
			return zero, err
		}
	}

	if pipelineConfig == "" {
		return zero, fmt.Errorf("pipeline config is empty for pipeline %s, cannot create secret", p.Spec.ID)
	}

	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels:    labels,
		},
		Immutable: ptrBool(true),
		StringData: map[string]string{
			"pipeline.json": pipelineConfig,
		},
		Type: v1.SecretTypeOpaque,
	}
	err = r.Create(ctx, &s, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			err = r.Get(ctx, namespacedName, &existingSecret)
			if err != nil {
				return zero, fmt.Errorf("secret was created concurrently, but failed to get it: %w", err)
			}
			return existingSecret, nil
		}
		return zero, fmt.Errorf("create secret %s: %w", namespacedName, err)
	}

	return s, nil
}

// updateSecret updates a secret by deleting and recreating it (since secrets are immutable)
func (r *PipelineReconciler) updateSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	// Check if a secret already exists
	var existingSecret v1.Secret
	err := r.Get(ctx, namespacedName, &existingSecret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.createSecret(ctx, namespacedName, labels, p)
		}
		return zero, fmt.Errorf("get existing secret %s: %w", namespacedName, err)
	}

	// Read from secret updated by Glassflow API
	var pipelineConfig string
	pipelineConfig, err = r.getPipelineConfigFromSecret(ctx, p.Spec.ID)
	if err != nil {
		if p.Spec.Config != "" {
			pipelineConfig = p.Spec.Config
		} else {
			return zero, err
		}
	}

	if pipelineConfig == "" {
		return zero, fmt.Errorf("pipeline config is empty for pipeline %s, cannot update secret", p.Spec.ID)
	}

	// recreate new secret
	err = r.Delete(ctx, &existingSecret)
	if err != nil {
		return zero, fmt.Errorf("delete existing secret %s: %w", namespacedName, err)
	}

	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels:    labels,
		},
		Immutable: ptrBool(true),
		StringData: map[string]string{
			"pipeline.json": pipelineConfig,
		},
		Type: v1.SecretTypeOpaque,
	}
	err = r.Create(ctx, &s, &client.CreateOptions{})
	if err != nil {
		return zero, fmt.Errorf("create updated secret %s: %w", namespacedName, err)
	}

	return s, nil
}

// deleteSecret deletes a secret for a pipeline
func (r *PipelineReconciler) deleteSecret(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	// ns deletion takes care of deleting the secret
	if r.PipelinesNamespaceAuto {
		log.Info("namespaceauto=true, skipping secret deletion", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
		return nil
	}

	secretName := types.NamespacedName{Namespace: r.PipelinesNamespaceName, Name: r.getResourceName(p, "secret")}
	var secret v1.Secret
	err := r.Get(ctx, secretName, &secret)
	if err != nil {
		log.Info("secret already deleted", "secret", secretName.Name)
		return nil
	}

	err = r.Delete(ctx, &secret)
	if err != nil {
		return fmt.Errorf("failed to delete secret %s: %w", secretName.Name, err)
	}

	return nil
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

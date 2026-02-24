package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
)

func (r *PipelineReconciler) terminatePipelineComponents(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(p)

	result, requeue, err := r.terminateIngestors(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.terminateDedups(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.terminateJoin(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.terminateSink(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	// All components are deleted
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) terminateIngestors(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	for i := range p.Spec.Ingestor.Streams {
		resourceName := r.getResourceName(p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		deleted, err := r.isStatefulSetAbsent(ctx, namespace, resourceName)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("check ingestor statefulset %s: %w", resourceName, err)
		}
		if !deleted {
			log.Info("deleting ingestor statefulset", "statefulset", resourceName, "namespace", namespace)
			var sts appsv1.StatefulSet
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName}, &sts)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get ingestor statefulset %s: %w", resourceName, err)
				}
			} else {
				if err = r.deleteStatefulSet(ctx, &sts); err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete ingestor statefulset %s: %w", resourceName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}

		svcAbsent, err := r.isServiceAbsent(ctx, namespace, resourceName)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("check ingestor service %s: %w", resourceName, err)
		}
		if !svcAbsent {
			log.Info("deleting ingestor headless service", "service", resourceName, "namespace", namespace)
			var svc v1.Service
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName}, &svc)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get ingestor service %s: %w", resourceName, err)
				}
			} else {
				if err = r.deleteService(ctx, &svc); err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete ingestor service %s: %w", resourceName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}
	}

	return ctrl.Result{}, false, nil
}

func (r *PipelineReconciler) terminateDedups(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(p, fmt.Sprintf("dedup-%d", i))
		deleted, err := r.isStatefulSetAbsent(ctx, namespace, dedupName)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("check dedup statefulset %s: %w", dedupName, err)
		}
		if !deleted {
			log.Info("deleting dedup statefulset", "statefulset", dedupName, "namespace", namespace)
			var sts appsv1.StatefulSet
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dedupName}, &sts)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get dedup statefulset %s: %w", dedupName, err)
				}
			} else {
				if err = r.deleteStatefulSet(ctx, &sts); err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete dedup statefulset %s: %w", dedupName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}

		svcAbsent, err := r.isServiceAbsent(ctx, namespace, dedupName)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("check dedup service %s: %w", dedupName, err)
		}
		if !svcAbsent {
			log.Info("deleting dedup headless service", "service", dedupName, "namespace", namespace)
			var svc v1.Service
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dedupName}, &svc)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get dedup service %s: %w", dedupName, err)
				}
			} else {
				if err = r.deleteService(ctx, &svc); err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete dedup service %s: %w", dedupName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}

		log.Info("dedup statefulset and service are already deleted", "statefulset", dedupName, "service", dedupName, "namespace", namespace)
	}

	return ctrl.Result{}, false, nil
}

func (r *PipelineReconciler) terminateJoin(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	if !p.Spec.Join.Enabled {
		return ctrl.Result{}, false, nil
	}

	joinName := r.getResourceName(p, constants.JoinComponent)
	deleted, err := r.isDeploymentAbsent(ctx, namespace, joinName)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check join deployment: %w", err)
	}
	if deleted {
		log.Info("join deployment is already deleted", "namespace", namespace)
		return ctrl.Result{}, false, nil
	}

	log.Info("deleting join deployment", "namespace", namespace)
	var deployment appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: joinName}, &deployment)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, fmt.Errorf("get join deployment: %w", err)
		}
	} else {
		if err = r.deleteDeployment(ctx, &deployment); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("delete join deployment: %w", err)
		}
	}

	return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
}

func (r *PipelineReconciler) terminateSink(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	sinkName := r.getResourceName(p, constants.SinkComponent)
	deleted, err := r.isStatefulSetAbsent(ctx, namespace, sinkName)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check sink statefulset: %w", err)
	}
	if !deleted {
		log.Info("deleting sink statefulset", "namespace", namespace)
		var sts appsv1.StatefulSet
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: sinkName}, &sts)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, false, fmt.Errorf("get sink statefulset: %w", err)
			}
		} else {
			if err = r.deleteStatefulSet(ctx, &sts); err != nil {
				return ctrl.Result{}, false, fmt.Errorf("delete sink statefulset: %w", err)
			}
		}
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
	}

	svcAbsent, err := r.isServiceAbsent(ctx, namespace, sinkName)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check sink service: %w", err)
	}
	if svcAbsent {
		return ctrl.Result{}, false, nil
	}

	log.Info("deleting sink headless service", "namespace", namespace)
	var svc v1.Service
	err = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: sinkName}, &svc)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, fmt.Errorf("get sink service: %w", err)
		}
	} else {
		if err = r.deleteService(ctx, &svc); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("delete sink service: %w", err)
		}
	}

	return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
}

// -------------------------------------------------------------------------------------------------------------------

// createPipelineComponents ensures all required components are ready: Sink StatefulSet, Join deployment (if enabled), Dedup StatefulSets, Ingestor StatefulSets.

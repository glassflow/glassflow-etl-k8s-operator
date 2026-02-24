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
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
)

func (r *PipelineReconciler) stopPipelineComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	result, requeue, err := r.stopIngestorComponents(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.stopDedupComponents(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.stopJoinComponent(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	result, requeue, err = r.stopSinkComponents(ctx, log, p, namespace)
	if err != nil || requeue {
		return result, err
	}

	// All components (ingestor/sink StatefulSets and Services, Join deployment) are deleted
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) stopIngestorComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	for i := range p.Spec.Ingestor.Streams {
		resourceName := r.getResourceName(*p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		deleted, err := r.isStatefulSetAbsent(ctx, namespace, resourceName)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("check ingestor statefulset %s: %w", resourceName, err)
		}
		if !deleted {
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				result, err := r.handleOperationTimeout(ctx, log, p)
				return result, true, err
			}
			log.Info("deleting ingestor statefulset", "statefulset", resourceName, "namespace", namespace)
			var sts appsv1.StatefulSet
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName}, &sts)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get ingestor statefulset %s: %w", resourceName, err)
				}
			} else {
				err = r.deleteStatefulSet(ctx, &sts)
				if err != nil {
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
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				result, err := r.handleOperationTimeout(ctx, log, p)
				return result, true, err
			}
			log.Info("deleting ingestor headless service", "service", resourceName, "namespace", namespace)
			var svc v1.Service
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName}, &svc)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, false, fmt.Errorf("get ingestor service %s: %w", resourceName, err)
				}
			} else {
				err = r.deleteService(ctx, &svc)
				if err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete ingestor service %s: %w", resourceName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}
	}

	return ctrl.Result{}, false, nil
}

func (r *PipelineReconciler) stopDedupComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(*p, fmt.Sprintf("dedup-%d", i))

		// Check for timeout
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			result, err := r.handleOperationTimeout(ctx, log, p)
			return result, true, err
		}

		// Check for pending messages first
		err := r.checkDedupPendingMessages(ctx, *p, i)
		if err != nil {
			if errs.IsConsumerPendingMessagesError(err) {
				log.Info("dedup has pending messages, requeuing", "pipeline_id", p.Spec.ID, "stream_index", i, "error", err.Error())
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, true, nil
			}
			return ctrl.Result{}, false, fmt.Errorf("check dedup pending messages for stream %d: %w", i, err)
		}

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
				err = r.deleteStatefulSet(ctx, &sts)
				if err != nil {
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
				err = r.deleteService(ctx, &svc)
				if err != nil {
					return ctrl.Result{}, false, fmt.Errorf("delete dedup service %s: %w", dedupName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
		}

		log.Info("dedup statefulset and service are already deleted", "statefulset", dedupName, "service", dedupName, "namespace", namespace)
	}

	return ctrl.Result{}, false, nil
}

func (r *PipelineReconciler) stopJoinComponent(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	if !p.Spec.Join.Enabled {
		return ctrl.Result{}, false, nil
	}

	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		result, err := r.handleOperationTimeout(ctx, log, p)
		return result, true, err
	}

	err := r.checkJoinPendingMessages(ctx, *p)
	if err != nil {
		if errs.IsConsumerPendingMessagesError(err) {
			log.Info("join has pending messages, requeuing operation",
				"pipeline_id", p.Spec.ID,
				"error", err.Error())
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, true, nil
		}
		return ctrl.Result{}, false, fmt.Errorf("check join pending messages: %w", err)
	}

	deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(*p, constants.JoinComponent))
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check join deployment: %w", err)
	}
	if deleted {
		log.Info("join deployment is already deleted", "namespace", namespace)
		return ctrl.Result{}, false, nil
	}

	timedOut, _ = r.checkOperationTimeout(log, p)
	if timedOut {
		result, err := r.handleOperationTimeout(ctx, log, p)
		return result, true, err
	}

	log.Info("deleting join deployment", "namespace", namespace)
	var deployment appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(*p, constants.JoinComponent)}, &deployment)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, fmt.Errorf("get join deployment: %w", err)
		}
	} else {
		err = r.deleteDeployment(ctx, &deployment)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("delete join deployment: %w", err)
		}
	}

	// Requeue to wait for deployment to be fully deleted
	return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
}

func (r *PipelineReconciler) stopSinkComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, namespace string) (ctrl.Result, bool, error) {
	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		result, err := r.handleOperationTimeout(ctx, log, p)
		return result, true, err
	}
	err := r.checkSinkPendingMessages(ctx, *p)
	if err != nil {
		if errs.IsConsumerPendingMessagesError(err) {
			log.Info("sink has pending messages, requeuing operation",
				"pipeline_id", p.Spec.ID,
				"error", err.Error())
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, true, nil
		}
		return ctrl.Result{}, false, fmt.Errorf("check sink pending messages: %w", err)
	}

	sinkName := r.getResourceName(*p, constants.SinkComponent)
	deleted, err := r.isStatefulSetAbsent(ctx, namespace, sinkName)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check sink statefulset: %w", err)
	}
	if !deleted {
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			result, err := r.handleOperationTimeout(ctx, log, p)
			return result, true, err
		}
		log.Info("deleting sink statefulset", "namespace", namespace)
		var sts appsv1.StatefulSet
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: sinkName}, &sts)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, false, fmt.Errorf("get sink statefulset: %w", err)
			}
		} else {
			err = r.deleteStatefulSet(ctx, &sts)
			if err != nil {
				return ctrl.Result{}, false, fmt.Errorf("delete sink statefulset: %w", err)
			}
		}
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
	}
	svcAbsent, err := r.isServiceAbsent(ctx, namespace, sinkName)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("check sink service: %w", err)
	}
	if !svcAbsent {
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			result, err := r.handleOperationTimeout(ctx, log, p)
			return result, true, err
		}
		log.Info("deleting sink headless service", "namespace", namespace)
		var svc v1.Service
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: sinkName}, &svc)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, false, fmt.Errorf("get sink service: %w", err)
			}
		} else {
			err = r.deleteService(ctx, &svc)
			if err != nil {
				return ctrl.Result{}, false, fmt.Errorf("delete sink service: %w", err)
			}
		}
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, true, nil
	}

	return ctrl.Result{}, false, nil
}

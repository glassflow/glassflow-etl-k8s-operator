package controller

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

// newTestReconciler creates a PipelineReconciler configured for integration tests.
// NATS at localhost:4222 is required; postgres is intentionally omitted.
func newTestReconciler(nc *nats.NATSClient) *PipelineReconciler {
	return &PipelineReconciler{
		Client:     k8sClient,
		Scheme:     k8sClient.Scheme(),
		NATSClient: nc,
		Config: ReconcilerConfig{
			NATS: NATSSettings{ComponentAddr: "nats://nats.default.svc.cluster.local:4222"},
			Images: ComponentImages{
				Ingestor: "glassflow-noop-component:test",
				Join:     "glassflow-noop-component:test",
				Sink:     "glassflow-noop-component:test",
				Dedup:    "glassflow-noop-component:test",
			},
			PullPolicies: ComponentPullPolicies{
				Ingestor: "IfNotPresent",
				Join:     "IfNotPresent",
				Sink:     "IfNotPresent",
				Dedup:    "IfNotPresent",
			},
			Namespaces:         PipelineNamespaces{Auto: true},
			GlassflowNamespace: "default",
			Observability: ComponentObservability{
				LogsEnabled:    "false",
				MetricsEnabled: "false",
				OTelEndpoint:   "http://localhost:4318",
				LogLevels:      ComponentLogLevels{Ingestor: "info", Join: "info", Sink: "info", Dedup: "info"},
				ImageTags:      ComponentImageTags{Ingestor: "test", Join: "test", Sink: "test", Dedup: "test"},
			},
			DedupStorage: DedupStorageDefaults{Size: "1Gi"},
			ResourceDefaults: ComponentResourceDefaults{
				Ingestor: ResourceDefaults{CPURequest: "100m", CPULimit: "200m", MemoryRequest: "128Mi", MemoryLimit: "256Mi"},
				Sink:     ResourceDefaults{CPURequest: "100m", CPULimit: "200m", MemoryRequest: "128Mi", MemoryLimit: "256Mi"},
				Join:     ResourceDefaults{CPURequest: "100m", CPULimit: "200m", MemoryRequest: "128Mi", MemoryLimit: "256Mi"},
				Dedup:    ResourceDefaults{CPURequest: "100m", CPULimit: "200m", MemoryRequest: "256Mi", MemoryLimit: "512Mi"},
			},
		},
	}
}

// patchStatefulSetsToReady sets ReadyReplicas == Spec.Replicas for all StatefulSets
// in the given namespace. envtest has no kubelet, so we do this manually to unblock
// the cascade (sink → join → dedup → ingestor).
func patchStatefulSetsToReady(ctx context.Context, g Gomega, namespace string) {
	var stsList appsv1.StatefulSetList
	g.Expect(k8sClient.List(ctx, &stsList, client.InNamespace(namespace))).To(Succeed())
	for i := range stsList.Items {
		sts := &stsList.Items[i]
		if sts.Spec.Replicas == nil || sts.Status.ReadyReplicas == *sts.Spec.Replicas {
			continue
		}
		patch := sts.DeepCopy()
		patch.Status.ReadyReplicas = *sts.Spec.Replicas
		patch.Status.Replicas = *sts.Spec.Replicas
		patch.Status.AvailableReplicas = *sts.Spec.Replicas
		if err := k8sClient.Status().Update(ctx, patch); err != nil && !apierrors.IsConflict(err) {
			g.Expect(err).NotTo(HaveOccurred())
		}
	}
}

// driveToRunning keeps reconciling (patching StatefulSet statuses each time) until
// the pipeline status is "Running" or the Eventually timeout is reached.
func driveToRunning(ctx context.Context, reconciler *PipelineReconciler, pipelineRef types.NamespacedName, pipelineNS string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		patchStatefulSetsToReady(ctx, g, pipelineNS)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: pipelineRef})
		g.Expect(err).NotTo(HaveOccurred())
		var p etlv1alpha1.Pipeline
		g.Expect(k8sClient.Get(ctx, pipelineRef, &p)).To(Succeed())
		g.Expect(string(p.Status)).To(Equal("Running"))
	}, "60s", "500ms").Should(Succeed())
}

var _ = Describe("Pipeline Controller", Ordered, func() {
	var (
		nc  *nats.NATSClient
		err error
	)

	BeforeAll(func() {
		nc, err = nats.New(ctx, nats.Config{
			URL:                "nats://localhost:4222",
			MaxAge:             nats.DefaultStreamMaxAge,
			MaxBytes:           int64(10 * 1024 * 1024), // 10MB — enough for tests, avoids NATS storage limit errors
			MaxMsgs:            int64(1000),
			Retention:          nats.ParseRetentionPolicy("WorkQueue"),
			AllowDirect:        true,
			AllowAtomicPublish: true,
		})
		Expect(err).NotTo(HaveOccurred(), "NATS must be running at localhost:4222 for integration tests")
	})

	Context("Simple kafka pipeline (1 ingestor, 1 sink)", func() {
		const pipelineID = "test-simple-01"
		pipelineRef := types.NamespacedName{Name: pipelineID, Namespace: "default"}
		pipelineNS := "pipeline-" + pipelineID

		AfterEach(func() {
			// Clean up Pipeline CR
			p := &etlv1alpha1.Pipeline{}
			if err := k8sClient.Get(ctx, pipelineRef, p); err == nil {
				// Remove finalizer to allow deletion
				p.Finalizers = nil
				_ = k8sClient.Update(ctx, p)
				_ = k8sClient.Delete(ctx, p)
			}
			// Clean up namespace
			ns := &v1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineNS}, ns); err == nil {
				_ = k8sClient.Delete(ctx, ns)
			}
			// Clean up NATS streams
			_ = nc.DeleteAllPipelineStreams(ctx, generatePipelineHash(pipelineID))
		})

		It("creates namespace, NATS streams, StatefulSets, and Services", func() {
			p := simplePipeline(pipelineID, 1, 1)
			Expect(k8sClient.Create(ctx, p)).To(Succeed())

			reconciler := newTestReconciler(nc)
			driveToRunning(ctx, reconciler, pipelineRef, pipelineNS)

			By("namespace exists")
			var ns v1.Namespace
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pipelineNS}, &ns)).To(Succeed())

			By("sink StatefulSet exists with 1 replica")
			var sinkSTS appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "sink"}, &sinkSTS)).To(Succeed())
			Expect(*sinkSTS.Spec.Replicas).To(Equal(int32(1)))

			By("ingestor StatefulSet exists with 1 replica")
			var ingSTS appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-0"}, &ingSTS)).To(Succeed())
			Expect(*ingSTS.Spec.Replicas).To(Equal(int32(1)))

			By("headless Services exist")
			var svc v1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "sink"}, &svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-0"}, &svc)).To(Succeed())

			By("pipeline config secret exists in pipeline namespace")
			var secret v1.Secret
			secretName := newTestReconciler(nc).getResourceName(*p)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: secretName}, &secret)).To(Succeed())
			Expect(secret.Data).To(HaveKey("pipeline.json"))

			By("NATS streams exist")
			hash := generatePipelineHash(pipelineID)
			streams, listErr := nc.ListPipelineStreams(ctx, hash)
			Expect(listErr).NotTo(HaveOccurred())
			Expect(streams).NotTo(BeEmpty())
			// DLQ stream must always exist
			dlqName := getDLQStreamName(pipelineID)
			Expect(streams).To(ContainElement(dlqName))
			// Ingestor output stream (1 sink replica → 1 stream)
			ingestorStream := fmt.Sprintf("gfm-%s-ingestor-out_0", hash)
			Expect(streams).To(ContainElement(ingestorStream))
		})
	})

	Context("Join pipeline (2 ingestors, join, sink)", func() {
		const pipelineID = "test-join-01"
		pipelineRef := types.NamespacedName{Name: pipelineID, Namespace: "default"}
		pipelineNS := "pipeline-" + pipelineID

		AfterEach(func() {
			p := &etlv1alpha1.Pipeline{}
			if err := k8sClient.Get(ctx, pipelineRef, p); err == nil {
				p.Finalizers = nil
				_ = k8sClient.Update(ctx, p)
				_ = k8sClient.Delete(ctx, p)
			}
			ns := &v1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineNS}, ns); err == nil {
				_ = k8sClient.Delete(ctx, ns)
			}
			_ = nc.DeleteAllPipelineStreams(ctx, generatePipelineHash(pipelineID))
		})

		It("creates 2 ingestors, join, sink StatefulSets and NATS join KV stores", func() {
			p := joinPipeline(pipelineID, 1, 1, 1, 1)
			Expect(k8sClient.Create(ctx, p)).To(Succeed())

			reconciler := newTestReconciler(nc)
			driveToRunning(ctx, reconciler, pipelineRef, pipelineNS)

			By("sink StatefulSet exists")
			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "sink"}, &sts)).To(Succeed())

			By("join StatefulSet exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "join"}, &sts)).To(Succeed())

			By("left ingestor StatefulSet exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-0"}, &sts)).To(Succeed())

			By("right ingestor StatefulSet exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-1"}, &sts)).To(Succeed())

			By("NATS join output stream exists")
			hash := generatePipelineHash(pipelineID)
			streams, listErr := nc.ListPipelineStreams(ctx, hash)
			Expect(listErr).NotTo(HaveOccurred())
			joinStream := fmt.Sprintf("gfm-%s-join-out_0", hash)
			Expect(streams).To(ContainElement(joinStream))
		})
	})

	Context("Dedup pipeline (ingestor, dedup, sink)", func() {
		const pipelineID = "test-dedup-01"
		pipelineRef := types.NamespacedName{Name: pipelineID, Namespace: "default"}
		pipelineNS := "pipeline-" + pipelineID

		AfterEach(func() {
			p := &etlv1alpha1.Pipeline{}
			if err := k8sClient.Get(ctx, pipelineRef, p); err == nil {
				p.Finalizers = nil
				_ = k8sClient.Update(ctx, p)
				_ = k8sClient.Delete(ctx, p)
			}
			ns := &v1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineNS}, ns); err == nil {
				_ = k8sClient.Delete(ctx, ns)
			}
			_ = nc.DeleteAllPipelineStreams(ctx, generatePipelineHash(pipelineID))
		})

		It("creates ingestor, dedup, sink StatefulSets with PVC for dedup", func() {
			p := dedupPipeline(pipelineID)
			Expect(k8sClient.Create(ctx, p)).To(Succeed())

			reconciler := newTestReconciler(nc)
			driveToRunning(ctx, reconciler, pipelineRef, pipelineNS)

			By("sink StatefulSet exists")
			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "sink"}, &sts)).To(Succeed())

			By("dedup StatefulSet exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "dedup-0"}, &sts)).To(Succeed())
			Expect(sts.Spec.VolumeClaimTemplates).NotTo(BeEmpty(), "dedup StatefulSet must have PVC template")

			By("ingestor StatefulSet exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-0"}, &sts)).To(Succeed())

			By("NATS dedup output stream exists")
			hash := generatePipelineHash(pipelineID)
			streams, listErr := nc.ListPipelineStreams(ctx, hash)
			Expect(listErr).NotTo(HaveOccurred())
			dedupStream := fmt.Sprintf("gfm-%s-dedup-out_0", hash)
			Expect(streams).To(ContainElement(dedupStream))
		})
	})

	Context("Multi-replica pipeline (2 ingestors, 2 sinks)", func() {
		const pipelineID = "test-multi-01"
		pipelineRef := types.NamespacedName{Name: pipelineID, Namespace: "default"}
		pipelineNS := "pipeline-" + pipelineID

		AfterEach(func() {
			p := &etlv1alpha1.Pipeline{}
			if err := k8sClient.Get(ctx, pipelineRef, p); err == nil {
				p.Finalizers = nil
				_ = k8sClient.Update(ctx, p)
				_ = k8sClient.Delete(ctx, p)
			}
			ns := &v1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineNS}, ns); err == nil {
				_ = k8sClient.Delete(ctx, ns)
			}
			_ = nc.DeleteAllPipelineStreams(ctx, generatePipelineHash(pipelineID))
		})

		It("creates StatefulSets with correct replica counts and 2 NATS streams", func() {
			p := multiReplicaPipeline(pipelineID, 2)
			Expect(k8sClient.Create(ctx, p)).To(Succeed())

			reconciler := newTestReconciler(nc)
			driveToRunning(ctx, reconciler, pipelineRef, pipelineNS)

			By("sink StatefulSet has 2 replicas")
			var sinkSTS appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "sink"}, &sinkSTS)).To(Succeed())
			Expect(*sinkSTS.Spec.Replicas).To(Equal(int32(2)))

			By("ingestor StatefulSet has 2 replicas")
			var ingSTS appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNS, Name: "ingestor-0"}, &ingSTS)).To(Succeed())
			Expect(*ingSTS.Spec.Replicas).To(Equal(int32(2)))

			By("2 ingestor output NATS streams exist (one per sink replica)")
			hash := generatePipelineHash(pipelineID)
			streams, listErr := nc.ListPipelineStreams(ctx, hash)
			Expect(listErr).NotTo(HaveOccurred())
			Expect(streams).To(ContainElement(fmt.Sprintf("gfm-%s-ingestor-out_0", hash)))
			Expect(streams).To(ContainElement(fmt.Sprintf("gfm-%s-ingestor-out_1", hash)))
		})
	})
})

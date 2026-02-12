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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

var _ = Describe("Pipeline Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-pipeline"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		pipeline := &etlv1alpha1.Pipeline{}

		nc, err := nats.New(ctx, nats.Config{
			URL:                "http://localhost:4222",
			MaxAge:             nats.DefaultStreamMaxAge,
			MaxBytes:           nats.DefaultStreamMaxBytes,
			Retention:          nats.ParseRetentionPolicy("WorkQueue"),
			AllowDirect:        true,
			AllowAtomicPublish: true,
		})
		Expect(err).NotTo(HaveOccurred())

		BeforeEach(func() {
			By("creating the custom resource for the Kind Pipeline")
			err := k8sClient.Get(ctx, typeNamespacedName, pipeline)
			if err != nil && errors.IsNotFound(err) {
				resource := &etlv1alpha1.Pipeline{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: etlv1alpha1.PipelineSpec{
						ID:  resourceName,
						DLQ: resourceName + "-DLQ",
						Ingestor: etlv1alpha1.Sources{
							Type: "kafka",
							Streams: []etlv1alpha1.SourceStream{
								{
									TopicName:    "test_topic1",
									OutputStream: "test_topic1",
									DedupWindow:  2 * time.Hour,
									Replicas:     1,
								},
								{
									TopicName:    "test_topic2",
									OutputStream: "test_topic2",
									DedupWindow:  5 * time.Minute,
									Replicas:     1,
								},
							},
						},
						Join: etlv1alpha1.Join{
							Type:         "temporal",
							OutputStream: "gf-stream-joined",
							Replicas:     1,
							Enabled:      true,
						},
						Sink: etlv1alpha1.Sink{
							Type:     "clickhouse",
							Replicas: 1,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &etlv1alpha1.Pipeline{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Pipeline")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &PipelineReconciler{
				Client:                      k8sClient,
				Scheme:                      k8sClient.Scheme(),
				NATSClient:                  nc,
				ComponentNATSAddr:           "nats://nats.default.svc.cluster.local:4222",
				IngestorImage:               "ghcr.io/glassflow/glassflow-etl-ingestor:latest",
				JoinImage:                   "ghcr.io/glassflow/glassflow-etl-join:latest",
				SinkImage:                   "ghcr.io/glassflow/glassflow-etl-sink:latest",
				IngestorPullPolicy:          "IfNotPresent",
				JoinPullPolicy:              "IfNotPresent",
				SinkPullPolicy:              "IfNotPresent",
				DedupPullPolicy:             "IfNotPresent",
				ObservabilityLogsEnabled:    "true",
				ObservabilityMetricsEnabled: "true",
				ObservabilityOTelEndpoint:   "http://otel-collector.observability.svc.cluster.local:4318",
				IngestorLogLevel:            "info",
				JoinLogLevel:                "info",
				SinkLogLevel:                "info",
				IngestorImageTag:            "latest",
				JoinImageTag:                "latest",
				SinkImageTag:                "latest",
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

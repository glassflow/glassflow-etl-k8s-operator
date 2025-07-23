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

	etlv1alpha1 "glassflow.io/pipeline/api/v1alpha1"
	"glassflow.io/pipeline/internal/nats"
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

		nc, err := nats.New("http://localhost:4222")
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
						ID: resourceName,
						Mapper: etlv1alpha1.MapperConfig{
							Type: "jsonToClickhouse",
							Streams: []etlv1alpha1.StreamSchema{{
								Name: "test_topic1",
								Config: etlv1alpha1.StreamSchemaConfig{
									Fields: []etlv1alpha1.StreamDataField{
										{
											FieldName: "id",
											FieldType: "string",
										},
										{
											FieldName: "name",
											FieldType: "string",
										},
									},
									JoinKeyField:    "id",
									JoinOrientation: "left",
									JoinWindow:      5 * time.Second,
								},
							}, {
								Name: "test_topic2",
								Config: etlv1alpha1.StreamSchemaConfig{
									Fields: []etlv1alpha1.StreamDataField{
										{
											FieldName: "order_id",
											FieldType: "string",
										},
										{
											FieldName: "customer_id",
											FieldType: "string",
										},
									},
									JoinKeyField:    "order_id",
									JoinOrientation: "right",
									JoinWindow:      5 * time.Second,
								},
							}},
							SinkMapping: []etlv1alpha1.SinkMappingConfig{
								{
									ColumnName: "id",
									StreamName: "test_topic1",
									FieldName:  "id",
									ColumnType: "string",
								},
								{
									ColumnName: "customer_name",
									StreamName: "test_topic2",
									FieldName:  "customer_name",
									ColumnType: "string",
								},
							},
						},
						Ingestor: etlv1alpha1.IngestorOperatorConfig{
							Type: "kafka",
							KafkaConnectionParams: etlv1alpha1.KafkaConnectionParamsConfig{
								Brokers:       []string{"localhost:9092"},
								SkipAuth:      false,
								SASLTLSEnable: false,
								SASLProtocol:  "PLAINTEXT",
								SASLMechanism: "PLAIN",
							},
							KafkaTopics: []etlv1alpha1.KafkaTopicsConfig{
								{
									Name:                       "test_topic1",
									ConsumerGroupInitialOffset: "earliest",
									Deduplication: etlv1alpha1.DeduplicationConfig{
										Enabled: true,
										ID:      "id",
										Type:    "string",
										Window:  7 * time.Hour,
									},
								},
								{
									Name:                       "test_topic2",
									ConsumerGroupInitialOffset: "earliest",
									Deduplication: etlv1alpha1.DeduplicationConfig{
										Enabled: true,
										ID:      "order_id",
										Type:    "string",
										Window:  7 * time.Hour,
									},
								},
							},
						},
						Join: etlv1alpha1.JoinOperatorConfig{
							Type:    "temporal",
							Enabled: true,
							Sources: []etlv1alpha1.JoinSourceConfig{
								{
									SourceID:    "test_topic1",
									JoinKey:     "id",
									Window:      5 * time.Second,
									Orientation: "left",
								},
								{
									SourceID:    "test_topic2",
									JoinKey:     "order_id",
									Window:      5 * time.Second,
									Orientation: "right",
								},
							},
						},
						Sink: etlv1alpha1.SinkOperatorConfig{
							Type:         "clickhouse",
							MaxBatchSize: 1000,
							MaxDelayTime: 3 * time.Minute,
							Host:         "localhost://clickhouse",
							Port:         "1234",
							DB:           "analytics",
							User:         "test",
							Password:     "test",
							Table:        "customer_orders",
							Secure:       false,
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
				Client:            k8sClient,
				Scheme:            k8sClient.Scheme(),
				NATSClient:        nc,
				ComponentNATSAddr: "nats://nats.default.svc.cluster.local:4222",
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

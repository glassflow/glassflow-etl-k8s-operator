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

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/glassflow/glassflow-etl-k8s-operator/test/utils"
)

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", managerNamespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", managerNamespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", managerNamespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", managerNamespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", managerNamespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})
	})
})

// pipelineStatus polls the Pipeline CR and returns its .status field.
func pipelineStatus(name, ns string) func(g Gomega) string { //nolint:unparam
	return func(g Gomega) string {
		cmd := exec.Command("kubectl", "get", "pipeline", name,
			"-n", ns, "-o", "jsonpath={.status}")
		out, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		return out
	}
}

// kubectlApplyPipelineCR creates or updates a Pipeline CR from a YAML string.
func kubectlApplyPipelineCR(yaml string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	_, err := utils.Run(cmd)
	return err
}

// kubectlAnnotatePipeline adds an annotation to a Pipeline CR.
func kubectlAnnotatePipeline(name, ns, annotation string) error { //nolint:unparam
	cmd := exec.Command("kubectl", "annotate", "--overwrite", "pipeline", name,
		"-n", ns, annotation)
	_, err := utils.Run(cmd)
	return err
}

// simplePipelineYAML returns a minimal Pipeline CR YAML for e2e tests.
func simplePipelineYAML(name, pipelineID string) string {
	return fmt.Sprintf(`apiVersion: etl.glassflow.io/v1alpha1
kind: Pipeline
metadata:
  name: %s
  namespace: default
  annotations:
    pipeline.etl.glassflow.io/create: "create"
spec:
  pipeline_id: %s
  config: '{"pipeline_id":"%s","sources":[],"sink":{"type":"clickhouse"}}'
  sources:
    type: kafka
    topics:
    - topic_name: e2e-topic-%s
      dedup_window: 0
  join:
    type: ""
    enabled: false
  sink:
    type: clickhouse
  pipeline_resources:
    ingestor:
      base:
        replicas: 1
    sink:
      replicas: 1
`, name, pipelineID, pipelineID, pipelineID)
}

func joinPipelineYAML(name, pipelineID string) string {
	return fmt.Sprintf(`apiVersion: etl.glassflow.io/v1alpha1
kind: Pipeline
metadata:
  name: %s
  namespace: default
  annotations:
    pipeline.etl.glassflow.io/create: "create"
spec:
  pipeline_id: %s
  config: '{"pipeline_id":"%s","sources":[],"sink":{"type":"clickhouse"}}'
  sources:
    type: kafka
    topics:
    - topic_name: e2e-left-%s
      dedup_window: 0
    - topic_name: e2e-right-%s
      dedup_window: 0
  join:
    type: temporal
    enabled: true
    left_buffer_ttl: 300000000000
    right_buffer_ttl: 300000000000
  sink:
    type: clickhouse
  pipeline_resources:
    ingestor:
      left:
        replicas: 1
      right:
        replicas: 1
    join:
      replicas: 1
    sink:
      replicas: 1
`, name, pipelineID, pipelineID, pipelineID, pipelineID)
}

var _ = Describe("Pipeline", Ordered, func() {
	SetDefaultEventuallyTimeout(5 * time.Minute)
	SetDefaultEventuallyPollingInterval(3 * time.Second)

	Context("Simple kafka pipeline lifecycle", func() {
		const (
			pipelineName = "e2e-simple"
			pipelineID   = "e2e-simple"
			pipelineNS   = "default"
			componentNS  = "pipeline-" + pipelineID
		)

		AfterAll(func() {
			// Force cleanup regardless of test outcome
			_ = kubectlAnnotatePipeline(pipelineName, pipelineNS, "pipeline.etl.glassflow.io/delete=delete")
			time.Sleep(5 * time.Second)
			cmd := exec.Command("kubectl", "delete", "pipeline", pipelineName, "-n", pipelineNS, "--ignore-not-found")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "ns", componentNS, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		It("should create namespace, StatefulSets, and reach Running status", func() {
			Expect(kubectlApplyPipelineCR(simplePipelineYAML(pipelineName, pipelineID))).To(Succeed())

			By("waiting for pipeline to reach Running status")
			Eventually(func(g Gomega) {
				status := pipelineStatus(pipelineName, pipelineNS)(g)
				if status != "Running" {
					cmd := exec.Command("kubectl", "logs", "-l", "control-plane=controller-manager",
						"-n", managerNamespace, "--tail=20")
					if logs, err := utils.Run(cmd); err == nil {
						_, _ = fmt.Fprintf(GinkgoWriter, "operator logs:\n%s\n", logs)
					}
				}
				g.Expect(status).To(Equal("Running"))
			}).Should(Succeed())

			By("verifying pipeline namespace exists")
			cmd := exec.Command("kubectl", "get", "ns", componentNS)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying sink StatefulSet exists")
			cmd = exec.Command("kubectl", "get", "statefulset", "sink", "-n", componentNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying ingestor StatefulSet exists")
			cmd = exec.Command("kubectl", "get", "statefulset", "ingestor-0", "-n", componentNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying headless services exist")
			cmd = exec.Command("kubectl", "get", "svc", "sink", "-n", componentNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should stop pipeline", func() {
			Expect(kubectlAnnotatePipeline(pipelineName, pipelineNS, "pipeline.etl.glassflow.io/stop=stop")).To(Succeed())

			By("waiting for pipeline to reach Stopped status")
			Eventually(func(g Gomega) {
				status := pipelineStatus(pipelineName, pipelineNS)(g)
				g.Expect(status).To(Equal("Stopped"))
			}).Should(Succeed())
		})

		It("should resume pipeline", func() {
			Expect(kubectlAnnotatePipeline(pipelineName, pipelineNS, "pipeline.etl.glassflow.io/resume=resume")).To(Succeed())

			By("waiting for pipeline to leave Stopped status")
			Eventually(func(g Gomega) {
				status := pipelineStatus(pipelineName, pipelineNS)(g)
				g.Expect(status).NotTo(Equal("Stopped"))
			}).Should(Succeed())
		})

		It("should delete pipeline and clean up namespace", func() {
			Expect(kubectlAnnotatePipeline(pipelineName, pipelineNS, "pipeline.etl.glassflow.io/delete=delete")).To(Succeed())

			By("waiting for pipeline namespace to be deleted")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "ns", componentNS)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "namespace should be gone")
			}).Should(Succeed())
		})
	})

	Context("Join pipeline", func() {
		const (
			pipelineName = "e2e-join"
			pipelineID   = "e2e-join"
			pipelineNS   = "default"
			componentNS  = "pipeline-" + pipelineID
		)

		AfterAll(func() {
			_ = kubectlAnnotatePipeline(pipelineName, pipelineNS, "pipeline.etl.glassflow.io/delete=delete")
			time.Sleep(5 * time.Second)
			cmd := exec.Command("kubectl", "delete", "pipeline", pipelineName, "-n", pipelineNS, "--ignore-not-found")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "ns", componentNS, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		It("should create 2 ingestors, join, sink and reach Running status", func() {
			Expect(kubectlApplyPipelineCR(joinPipelineYAML(pipelineName, pipelineID))).To(Succeed())

			By("waiting for pipeline to reach Running status")
			Eventually(func(g Gomega) {
				status := pipelineStatus(pipelineName, pipelineNS)(g)
				g.Expect(status).To(Equal("Running"))
			}).Should(Succeed())

			By("verifying join StatefulSet exists")
			cmd := exec.Command("kubectl", "get", "statefulset", "join", "-n", componentNS)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying left ingestor StatefulSet exists")
			cmd = exec.Command("kubectl", "get", "statefulset", "ingestor-0", "-n", componentNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying right ingestor StatefulSet exists")
			cmd = exec.Command("kubectl", "get", "statefulset", "ingestor-1", "-n", componentNS)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

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
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/glassflow/glassflow-etl-k8s-operator/test/utils"
)

// managerNamespace is the namespace where the operator is deployed.
// Shared between suite setup (BeforeSuite/AfterSuite) and Manager tests.
const managerNamespace = "glassflow-etl-k8s-operator-system"

var (
	// Optional Environment Variables:
	// - CERT_MANAGER_INSTALL_SKIP=true: Skips CertManager installation during test setup.
	// This variable is useful if CertManager is already installed, avoiding
	// re-installation and conflicts.
	skipCertManagerInstall = os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true"
	// isCertManagerAlreadyInstalled will be set true when CertManager CRDs be found on the cluster
	isCertManagerAlreadyInstalled = false
	// isNATSAlreadyInstalled will be set true when a msg be published on test JS
	isNATSAlreadyInstalled = false
	// isPostgresAlreadyInstalled will be set true when the postgresql helm release exists
	isPostgresAlreadyInstalled = false

	// projectImage is the name of the image which will be build and loaded
	// with the code source changes to be tested.
	projectImage = "example.com/glassflow-etl-k8s-operator:v0.0.1"
)

// TestE2E runs the end-to-end (e2e) test suite for the project. These tests execute in an isolated,
// temporary environment to validate project changes with the the purposed to be used in CI jobs.
// The default setup requires Kind, builds/loads the Manager Docker image locally, and installs
// CertManager.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting glassflow-etl-k8s-operator integration test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the manager(Operator) binary")
	cmd := exec.Command("make", "build")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager(Operator) binary")

	By("building the manager(Operator) image")
	cmd = exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager(Operator) image")

	By("loading the manager(Operator) image on Kind")
	err = utils.LoadImageToKindClusterWithName(projectImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager(Operator) image into Kind")

	By("building the no-op component image")
	err = utils.BuildNoopImage()
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the no-op component image")

	By("loading the no-op component image on Kind")
	err = utils.LoadImageToKindClusterWithName(utils.NoopComponentImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the no-op component image into Kind")

	// Setup CertManager before the suite if not skipped and if not already installed
	if !skipCertManagerInstall {
		By("checking if cert manager is installed already")
		isCertManagerAlreadyInstalled = utils.IsCertManagerCRDsInstalled()
		if !isCertManagerAlreadyInstalled {
			_, _ = fmt.Fprintf(GinkgoWriter, "Installing CertManager...\n")
			Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "WARNING: CertManager is already installed. Skipping installation...\n")
		}
	}

	By("installing NATS")
	isNATSAlreadyInstalled = utils.IsNATSInstalled()
	if !isNATSAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Installing NATS...\n")
		Expect(utils.InstallNATS()).To(Succeed(), "Failed to install NATS")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "WARNING: NATS is already installed. Skipping installation...\n")
	}

	// Postgres is optional — operator now starts without it (delete ops skip postgres).
	// Install it anyway for completeness; tests don't depend on it directly.
	By("checking if postgres is installed already")
	isPostgresAlreadyInstalled = utils.IsPostgresInstalled()
	if !isPostgresAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Installing PostgreSQL...\n")
		if err := utils.InstallPostgres(); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "WARNING: PostgreSQL installation failed: %v — continuing without it\n", err)
		}
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "WARNING: PostgreSQL is already installed. Skipping installation...\n")
	}

	By("creating manager namespace")
	cmd = exec.Command("kubectl", "create", "ns", managerNamespace)
	_, _ = utils.Run(cmd) // ignore error if already exists

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", managerNamespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("deploying the controller-manager with e2e overlay (noop component images)")
	cmd = exec.Command("make", "deploy-e2e", fmt.Sprintf("IMG=%s", projectImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
})

var _ = AfterSuite(func() {
	By("undeploying the controller-manager")
	cmd := exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	By("removing manager namespace")
	cmd = exec.Command("kubectl", "delete", "ns", managerNamespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)

	// Teardown CertManager after the suite if not skipped and if it was not already installed
	if !skipCertManagerInstall && !isCertManagerAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Uninstalling CertManager...\n")
		utils.UninstallCertManager()
	}

	if !isNATSAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Uninstalling NATS...\n")
		utils.UninstallNATS()
	}

	if !isPostgresAlreadyInstalled {
		_, _ = fmt.Fprintf(GinkgoWriter, "Uninstalling PostgreSQL...\n")
		utils.UninstallPostgres()
	}
})

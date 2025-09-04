# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Component image configurations
INGESTOR_IMAGE ?= ghcr.io/glassflow/glassflow-etl-ingestor:stable
JOIN_IMAGE ?= ghcr.io/glassflow/glassflow-etl-join:stable
SINK_IMAGE ?= ghcr.io/glassflow/glassflow-etl-sink:stable

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# NATS port configuration variable
NATS_PORT ?= 4222
# Kind cluster name configuration variable
KIND_CLUSTER_NAME ?= kind

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# Prometheus and CertManager are installed by default; skip with:
# - PROMETHEUS_INSTALL_SKIP=true
# - CERT_MANAGER_INSTALL_SKIP=true
.PHONY: test-e2e
test-e2e: manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	@command -v kind >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@kind get clusters | grep -q '$(KIND_CLUSTER_NAME)' || { \
		echo "No Kind cluster with name '$(KIND_CLUSTER_NAME)' is running. Please start a Kind cluster before running the e2e tests."; \
		exit 1; \
	}
	go test ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go --nats-addr localhost:$(NATS_PORT) --ingestor-image $(INGESTOR_IMAGE) --join-image $(JOIN_IMAGE) --sink-image $(SINK_IMAGE)

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --load -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name glassflow-etl-k8s-operator-builder
	$(CONTAINER_TOOL) buildx use glassflow-etl-k8s-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) -t ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm glassflow-etl-k8s-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.5.0
CONTROLLER_TOOLS_VERSION ?= v0.17.1
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v1.63.4

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

# TODO: shouldn't randomly trust "latest" version of random lib
# fix this helmifying solution once POC is done - possible security
# vulnerability
HELMIFY ?= $(LOCALBIN)/helmify

.PHONY: helmify
helmify: $(HELMIFY) ## Download helmify locally if necessary.
$(HELMIFY): $(LOCALBIN)
	test -s $(LOCALBIN)/helmify || GOBIN=$(LOCALBIN) go install github.com/arttor/helmify/cmd/helmify@latest

.PHONY: helm
helm: manifests kustomize helmify
	$(KUSTOMIZE) build config/default | $(HELMIFY)

.PHONY: package-chart
package-chart: helm
	helm package chart -d dist/charts --version $(VERSION) --app-version $(APP_VERSION)

##@ Workflow Testing

.PHONY: workflow-test
workflow-test: ## Run complete workflow testing locally (equivalent to GitHub Actions)
	@echo "🚀 Starting complete workflow testing..."
	@echo "=========================================="
	
	@echo "📋 Step 1: Installing required tools..."
	@$(MAKE) golangci-lint
	@$(MAKE) kustomize
	@$(MAKE) controller-gen
	
	@echo "🔍 Step 2: Running linting..."
	@$(MAKE) lint
	
	@echo "🧪 Step 3: Running tests..."
	@$(MAKE) test
	
	@echo "🐳 Step 4: Building Docker image..."
	@$(MAKE) docker-buildx IMG=workflow-test:latest
	
	@echo "🔒 Step 5: Security scanning..."
	@$(MAKE) security-scan
	
	@echo "📦 Step 6: Validating manifests..."
	@$(MAKE) validate-manifests
	
	@echo "🏗️ Step 7: Testing cross-compilation..."
	@$(MAKE) test-cross-compilation
	
	@echo "📋 Step 8: Dependency checks..."
	@$(MAKE) dependency-check
	
	@echo "✅ All workflow tests completed successfully!"
	@echo "🎉 Your code is ready to push to GitHub!"

.PHONY: security-scan
security-scan: ## Run security scanning locally
	@echo "🔒 Running security scan..."
	@if command -v trivy >/dev/null 2>&1; then \
		echo "Scanning filesystem for vulnerabilities..."; \
		trivy fs --severity HIGH,CRITICAL . || echo "⚠️  Trivy found vulnerabilities - check output above"; \
		echo "Scanning Go dependencies..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
		govulncheck ./... || echo "⚠️  govulncheck found vulnerabilities - check output above"; \
	else \
		echo "⚠️  Trivy not installed. Install with: brew install trivy (macOS) or see https://aquasecurity.github.io/trivy/"; \
		echo "Running Go vulnerability check only..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
		govulncheck ./... || echo "⚠️  govulncheck found vulnerabilities - check output above"; \
	fi

.PHONY: validate-manifests
validate-manifests: ## Validate Kubernetes manifests
	@echo "📦 Validating Kubernetes manifests..."
	@$(MAKE) kustomize
	@echo "Validating CRDs..."
	@$(KUSTOMIZE) build config/crd > /dev/null || { echo "❌ CRD validation failed"; exit 1; }
	@echo "Validating default config..."
	@$(KUSTOMIZE) build config/default > /dev/null || { echo "❌ Default config validation failed"; exit 1; }
	@echo "✅ All manifests validated successfully"

.PHONY: test-cross-compilation
test-cross-compilation: ## Test cross-compilation for different platforms
	@echo "🏗️ Testing cross-compilation..."
	@echo "Building for AMD64..."
	@GOOS=linux GOARCH=amd64 go build -o bin/manager-amd64 cmd/main.go || { echo "❌ AMD64 build failed"; exit 1; }
	@echo "Building for ARM64..."
	@GOOS=linux GOARCH=arm64 go build -o bin/manager-arm64 cmd/main.go || { echo "❌ ARM64 build failed"; exit 1; }
	@echo "Building for PPC64LE..."
	@GOOS=linux GOARCH=ppc64le go build -o bin/manager-ppc64le cmd/main.go || { echo "❌ PPC64LE build failed"; exit 1; }
	@echo "Building for S390X..."
	@GOOS=linux GOARCH=s390x go build -o bin/manager-s390x cmd/main.go || { echo "❌ S390X build failed"; exit 1; }
	@echo "✅ All cross-compilation tests passed"
	@echo "Cleaning up build artifacts..."
	@rm -f bin/manager-*

.PHONY: dependency-check
dependency-check: ## Check Go dependencies for vulnerabilities and updates
	@echo "📋 Checking Go dependencies..."
	@echo "Checking for outdated dependencies..."
	@go list -u -m all | grep -E '\[.*\]' || echo "✅ All dependencies are up to date"
	@echo "Verifying dependencies..."
	@go mod verify || { echo "❌ Dependency verification failed"; exit 1; }
	@echo "✅ Dependency check completed"

.PHONY: quick-test
quick-test: ## Quick test for common issues (faster than full workflow)
	@echo "⚡ Running quick test..."
	@echo "🔍 Linting..."
	@$(MAKE) lint
	@echo "🧪 Unit tests..."
	@$(MAKE) test
	@echo "📦 Manifest validation..."
	@$(MAKE) validate-manifests
	@echo "✅ Quick test completed successfully!"

.PHONY: install-tools
install-tools: ## Install all required tools for workflow testing
	@echo "🛠️ Installing required tools..."
	@$(MAKE) golangci-lint
	@$(MAKE) kustomize
	@$(MAKE) controller-gen
	@echo "Installing Trivy for security scanning..."
	@if command -v brew >/dev/null 2>&1; then \
		brew install trivy; \
	elif command -v curl >/dev/null 2>&1; then \
		curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin; \
	else \
		echo "⚠️  Please install Trivy manually: https://aquasecurity.github.io/trivy/"; \
	fi
	@echo "✅ All tools installed successfully"
# GitHub Workflows

This directory contains GitHub Actions workflows for the GlassFlow ETL Kubernetes Operator project.

## Overview

The workflows are designed to provide a comprehensive CI/CD pipeline that includes:
- Building and testing Docker images
- Security scanning and vulnerability detection
- Multi-platform compatibility testing
- Automated releases and version management
- Dependency management and updates

## Workflows

### 1. CI/CD Pipeline (`ci-cd.yml`) - **Main Workflow**

**Trigger**: Push to `main` branch, pull requests

**Purpose**: Complete CI/CD pipeline that runs on every code change

**Jobs**:
- **lint**: Code linting with golangci-lint
- **test**: Unit tests with NATS integration
- **e2e-test**: End-to-end tests with Kind cluster
- **build**: Multi-platform Docker image building and pushing
- **security-scan**: Vulnerability scanning with Trivy
- **validate-manifests**: Kubernetes manifest validation
- **prepare-deployment**: Build installer and upload artifacts
- **notify**: Success/failure notifications

**Features**:
- Multi-platform builds (linux/amd64, linux/arm64)
- GitHub Actions cache for faster builds
- Security scanning with SARIF output
- Manifest validation
- Artifact uploads
- Automatic tagging: `main` and `stable` for main branch, `develop` for develop branch

### 2. Multi-Platform Build (`multi-platform-build.yml`)

**Trigger**: Push to `main` branch, pull requests, manual dispatch

**Purpose**: Comprehensive multi-platform testing and validation

**Jobs**:
- **build-multi-platform**: Matrix build for different architectures
- **test-cross-compilation**: Test Go cross-compilation

**Features**:
- Matrix strategy for multiple platforms (linux/amd64, linux/arm64, linux/ppc64le, linux/s390x)
- Cross-compilation testing
- Platform-specific image tags
- Binary compatibility testing

### 3. Security Scan (`security-scan.yml`)

**Trigger**: Weekly schedule (Mondays 9 AM UTC), manual dispatch

**Purpose**: Comprehensive security scanning of images and dependencies

**Jobs**:
- **security-scan**: Container image vulnerability scanning
- **dependency-scan**: Filesystem and dependency scanning

**Features**:
- Scheduled weekly scans
- Manual trigger with custom image tag
- Multiple scan types (vulnerabilities, secrets, config)
- SARIF output for GitHub Security tab
- Table format output for logs

### 4. Dependency Check (`dependency-check.yml`)

**Trigger**: Weekly schedule (Tuesdays 10 AM UTC), manual dispatch, go.mod changes

**Purpose**: Monitor and update Go dependencies

**Jobs**:
- **check-dependencies**: Check for outdated and vulnerable dependencies
- **update-dependencies**: Update dependencies and create PR

**Features**:
- Automated dependency checking
- Vulnerability scanning with govulncheck
- Automated PR creation for updates
- Weekly scheduled runs

### 5. Version Tag Release (`version-tag.yml`)

**Trigger**: Push of version tags (e.g., `v0.0.1`, `v1.0.0`)

**Purpose**: Create production releases with stable tags

**Jobs**:
- **version-release**: Build, tag, and release versioned images

**Features**:
- Automatic version extraction from git tags
- Creates `{version}` and `stable` tags
- Security scanning with Trivy
- GitHub releases with detailed notes
- Stable tag always points to latest version

### 6. Release Operator (`release-operator.yml`)

**Trigger**: Workflow call (used by other workflows)

**Purpose**: Create releases with versioned Docker images

**Jobs**:
- **calculate-version**: Determine new version number
- **release**: Build, tag, and release images

**Features**:
- Semantic versioning
- Multi-tag image releases (version, latest, commit hash)
- GitHub releases with detailed notes
- VERSION file updates

### 7. Release Chart (`release-chart.yml`)

**Trigger**: Workflow call

**Purpose**: Release Helm charts

**Features**:
- Chart versioning
- Cross-repository releases
- GitHub App token authentication

### 8. Calculate Version (`calculate-version.yml`)

**Trigger**: Workflow call

**Purpose**: Calculate semantic version numbers

**Features**:
- Version bump types (patch, minor, major)
- VERSION file management
- Output for other workflows

### 9. Release Coordinator (`release.yml`)

**Trigger**: Manual dispatch

**Purpose**: Coordinate releases of both operator and Helm chart

**Jobs**:
- **release-chart**: Release Helm chart with specified version bump
- **release-operator**: Release operator with specified version bump
- **notify**: Send release notifications

**Features**:
- Manual release coordination
- Version bump type selection
- Conditional releases
- Slack notifications

### 10. Notify (`notify.yaml`)

**Trigger**: Workflow call

**Purpose**: Send notifications

**Features**:
- Success/failure notifications
- Slack webhook integration
- Customizable notification content

## Usage

### Manual Workflow Dispatch

Most workflows can be triggered manually:

1. Go to the **Actions** tab in your repository
2. Select the workflow you want to run
3. Click **Run workflow**
4. Choose the branch and any required inputs
5. Click **Run workflow**

### Scheduled Workflows

- **Security Scan**: Runs every Monday at 9 AM UTC
- **Dependency Check**: Runs every Tuesday at 10 AM UTC

### Workflow Dependencies

Some workflows are designed to work together:

- `ci-cd.yml` is the main pipeline that orchestrates everything
- `release-operator.yml` is called by release workflows
- `calculate-version.yml` provides version management for releases

## Tagging Strategy

The workflows implement a comprehensive tagging strategy:

### Branch-based Tags
- **`main` branch**: Creates `main` tag
- **Other branches**: Creates `{branch-name}` tag

### Commit-based Tags
- **All branches**: Creates `{branch}-{commit-sha}` tags for traceability

### Version Release Tags
- **Version tag pushes** (e.g., `git tag v1.0.0 && git push origin v1.0.0`):
  - Creates `{version}` tag (e.g., `1.0.0`)
  - Creates `stable` tag (always points to latest version)
- **Stable tag**: Only created on version releases, points to latest stable version

### Tag Usage Recommendations
- **Production**: Use `stable` tag (most reliable - only updated on version releases)
- **Development**: Use `main` tag (updated on every main branch push)
- **Specific versions**: Use `{version}` tags (e.g., `1.0.0`)
- **Debugging**: Use `{branch}-{commit-sha}` tags for exact code state

### How to Create a Version Release
```bash
# 1. Create and push a version tag
git tag v1.0.0
git push origin v1.0.0

# 2. This automatically triggers:
#    - Builds Docker image with tags: 1.0.0 and stable
#    - Creates GitHub release
#    - Runs security scan
#    - Updates stable tag to point to 1.0.0
```

## Configuration

### Environment Variables

All workflows use these environment variables:
- `REGISTRY`: GitHub Container Registry (ghcr.io/glassflow)
- `IMAGE_NAME`: Docker image name (glassflow-etl-k8s-operator)

### Permissions

Workflows request minimal required permissions:
- `contents: read` - Read repository contents
- `packages: write` - Push to container registry
- `security-events: write` - Upload security scan results
- `pull-requests: write` - Create dependency update PRs

### Secrets

Required secrets:
- `GITHUB_TOKEN` - Automatically provided by GitHub
- `APP_ID` and `APP_PRIVATE_KEY` - For cross-repository releases
- `SLACK_WEBHOOK_URL` - For release notifications

## Best Practices

1. **Security First**: All images are scanned for vulnerabilities
2. **Multi-Platform**: Images are built for multiple architectures
3. **Caching**: GitHub Actions cache is used for faster builds
4. **Validation**: Manifests and CRDs are validated before deployment
5. **Automation**: Dependencies are automatically checked and updated
6. **Monitoring**: Comprehensive logging and notifications

## Troubleshooting

### Common Issues

1. **Build Failures**: Check Docker build logs and platform compatibility
2. **Test Failures**: Ensure NATS server is running and accessible
3. **Permission Errors**: Verify workflow permissions and secrets
4. **Cache Issues**: Clear GitHub Actions cache if builds are inconsistent

### Debug Mode

Some workflows include debug flags:
- Docker Buildx debug mode for detailed build information
- Verbose logging for troubleshooting

## Contributing

When adding new workflows:

1. Follow the existing naming conventions
2. Include comprehensive documentation
3. Request minimal required permissions
4. Add appropriate error handling
5. Include security scanning where applicable
6. Test thoroughly before committing

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docker Buildx](https://docs.docker.com/build/buildx/)
- [Trivy Security Scanner](https://aquasecurity.github.io/trivy/)
- [golangci-lint](https://golangci-lint.run/)
- [Kustomize](https://kustomize.io/)

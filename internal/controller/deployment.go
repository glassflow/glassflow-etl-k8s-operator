package controller

import (
	"encoding/json"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type containerBuilder interface {
	withName(name string) containerBuilder
	withImage(image string) containerBuilder
	withImagePullPolicy(pullPolicy string) containerBuilder
	withVolumeMount(mount v1.VolumeMount) containerBuilder
	withEnv(env []v1.EnvVar) containerBuilder
	withResources(cpuRequest, cpuLimit, memoryRequest, memoryLimit string) containerBuilder
	build() *v1.Container
}

type componentContainer struct {
	con           *v1.Container
	cpuRequest    string
	cpuLimit      string
	memoryRequest string
	memoryLimit   string
	pullPolicy    string
}

func newComponentContainerBuilder() *componentContainer {
	return &componentContainer{
		con: &v1.Container{},
	}
}

var _ containerBuilder = (*componentContainer)(nil)

// withEnv implements containerBuilder.
func (c *componentContainer) withEnv(env []v1.EnvVar) containerBuilder {
	c.con.Env = env
	return c
}

// withImage implements containerBuilder.
func (c *componentContainer) withImage(image string) containerBuilder {
	c.con.Image = image
	return c
}

// withImagePullPolicy implements containerBuilder.
func (c *componentContainer) withImagePullPolicy(pullPolicy string) containerBuilder {
	c.pullPolicy = pullPolicy
	return c
}

// withSecret implements containerBuilder.
func (c *componentContainer) withVolumeMount(mount v1.VolumeMount) containerBuilder {
	c.con.VolumeMounts = append(c.con.VolumeMounts, mount)
	return c
}

// withLabels implements deploymentBuilder.
func (c *componentContainer) withName(name string) containerBuilder {
	c.con.Name = name
	return c
}

// withResources implements containerBuilder.
func (c *componentContainer) withResources(cpuRequest, cpuLimit, memoryRequest, memoryLimit string) containerBuilder {
	c.cpuRequest = cpuRequest
	c.cpuLimit = cpuLimit
	c.memoryRequest = memoryRequest
	c.memoryLimit = memoryLimit
	return c
}

// build implements containerBuilder.
func (c *componentContainer) build() *v1.Container {
	// Parse resource strings and create resource requirements
	cpuRequest, _ := resource.ParseQuantity(c.cpuRequest)
	cpuLimit, _ := resource.ParseQuantity(c.cpuLimit)
	memoryRequest, _ := resource.ParseQuantity(c.memoryRequest)
	memoryLimit, _ := resource.ParseQuantity(c.memoryLimit)

	c.con.Resources = v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    cpuLimit,
			v1.ResourceMemory: memoryLimit,
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    cpuRequest,
			v1.ResourceMemory: memoryRequest,
		},
	}

	// Parse and set image pull policy
	if c.pullPolicy != "" {
		switch c.pullPolicy {
		case "Always":
			c.con.ImagePullPolicy = v1.PullAlways
		case "Never":
			c.con.ImagePullPolicy = v1.PullNever
		case "IfNotPresent":
			c.con.ImagePullPolicy = v1.PullIfNotPresent
		default:
			// Default to IfNotPresent if invalid value
			c.con.ImagePullPolicy = v1.PullIfNotPresent
		}
	} else {
		// Default to IfNotPresent if not specified
		c.con.ImagePullPolicy = v1.PullIfNotPresent
	}

	return c.con
}

type deploymentBuilder interface {
	withResourceName(name string) deploymentBuilder
	withNamespace(namespace v1.Namespace) deploymentBuilder
	withLabels(labels map[string]string) deploymentBuilder
	withVolume(volume v1.Volume) deploymentBuilder
	withContainer(container v1.Container) deploymentBuilder
	withReplicas(replicas int) deploymentBuilder
	withAffinity(affinityJSON string) deploymentBuilder
	build() *appsv1.Deployment
}

type componentDeployment struct {
	dep *appsv1.Deployment
}

func newComponentDeploymentBuilder() *componentDeployment {
	return &componentDeployment{
		dep: &appsv1.Deployment{},
	}
}

var _ deploymentBuilder = (*componentDeployment)(nil)

// withNamespace implements deploymentBuilder.
func (c *componentDeployment) withNamespace(namespace v1.Namespace) deploymentBuilder {
	c.dep.SetNamespace(namespace.GetName())
	return c
}

// withResourceName implements deploymentBuilder.
func (c *componentDeployment) withResourceName(name string) deploymentBuilder {
	c.dep.SetName(name)
	return c
}

// withLabels implements deploymentBuilder.
func (c *componentDeployment) withLabels(labels map[string]string) deploymentBuilder {
	c.dep.SetLabels(labels)
	return c
}

// withVolume implements deploymentBuilder.
func (c *componentDeployment) withVolume(volume v1.Volume) deploymentBuilder {
	c.dep.Spec.Template.Spec.Volumes = append(c.dep.Spec.Template.Spec.Volumes, volume)
	c.dep.Spec.Template.Spec.SecurityContext = &v1.PodSecurityContext{
		RunAsUser:  ptrInt64(1001),
		RunAsGroup: ptrInt64(1001),
		FSGroup:    ptrInt64(1001),
	}
	return c
}

// withContainer implements deploymentBuilder.
func (c *componentDeployment) withContainer(container v1.Container) deploymentBuilder {
	c.dep.Spec.Template.Spec.Containers = append(c.dep.Spec.Template.Spec.Containers, container)
	return c
}

// withReplicas implements deploymentBuilder.
func (c *componentDeployment) withReplicas(replicas int) deploymentBuilder {
	if replicas > 0 {
		c.dep.Spec.Replicas = ptrInt32(int32(replicas))
	}
	return c
}

// withAffinity implements deploymentBuilder.
func (c *componentDeployment) withAffinity(affinityJSON string) deploymentBuilder {
	// If no affinity is specified, return without setting anything (default scheduling)
	if affinityJSON == "" {
		return c
	}

	// Parse the JSON affinity configuration
	var affinity v1.Affinity
	if err := json.Unmarshal([]byte(affinityJSON), &affinity); err != nil {
		// If JSON parsing fails, log the error but don't fail the deployment
		// This ensures backward compatibility and graceful degradation
		return c
	}

	// Set the affinity on the deployment
	c.dep.Spec.Template.Spec.Affinity = &affinity
	return c
}

// build implements deploymentBuilder.
func (c *componentDeployment) build() *appsv1.Deployment {
	if c.dep.Spec.Replicas == nil {
		c.dep.Spec.Replicas = ptrInt32(1)
	}
	c.dep.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: c.dep.GetLabels(),
	}
	c.dep.Spec.Template.GetObjectMeta().SetLabels(c.dep.GetLabels())

	return c.dep
}

// -------------------------------------------------------------------------------------------------------------------
// StatefulSet Builder
// -------------------------------------------------------------------------------------------------------------------

type statefulSetBuilder interface {
	withResourceName(name string) statefulSetBuilder
	withNamespace(namespace v1.Namespace) statefulSetBuilder
	withServiceName(serviceName string) statefulSetBuilder
	withLabels(labels map[string]string) statefulSetBuilder
	withVolume(volume v1.Volume) statefulSetBuilder
	withContainer(container v1.Container) statefulSetBuilder
	withReplicas(replicas int) statefulSetBuilder
	withAffinity(affinityJSON string) statefulSetBuilder
	withVolumeClaimTemplate(pvc v1.PersistentVolumeClaim) statefulSetBuilder
	build() *appsv1.StatefulSet
}

type componentStatefulSet struct {
	sts *appsv1.StatefulSet
}

func newComponentStatefulSetBuilder() *componentStatefulSet {
	return &componentStatefulSet{
		sts: &appsv1.StatefulSet{},
	}
}

var _ statefulSetBuilder = (*componentStatefulSet)(nil)

// withNamespace implements statefulSetBuilder.
func (c *componentStatefulSet) withNamespace(namespace v1.Namespace) statefulSetBuilder {
	c.sts.SetNamespace(namespace.GetName())
	return c
}

// withResourceName implements statefulSetBuilder.
func (c *componentStatefulSet) withResourceName(name string) statefulSetBuilder {
	c.sts.SetName(name)
	return c
}

// withServiceName implements statefulSetBuilder.
func (c *componentStatefulSet) withServiceName(serviceName string) statefulSetBuilder {
	c.sts.Spec.ServiceName = serviceName
	return c
}

// withLabels implements statefulSetBuilder.
func (c *componentStatefulSet) withLabels(labels map[string]string) statefulSetBuilder {
	c.sts.SetLabels(labels)
	return c
}

// withVolume implements statefulSetBuilder.
func (c *componentStatefulSet) withVolume(volume v1.Volume) statefulSetBuilder {
	c.sts.Spec.Template.Spec.Volumes = append(c.sts.Spec.Template.Spec.Volumes, volume)
	c.sts.Spec.Template.Spec.SecurityContext = &v1.PodSecurityContext{
		RunAsUser:  ptrInt64(1001),
		RunAsGroup: ptrInt64(1001),
		FSGroup:    ptrInt64(1001),
	}
	return c
}

// withContainer implements statefulSetBuilder.
func (c *componentStatefulSet) withContainer(container v1.Container) statefulSetBuilder {
	c.sts.Spec.Template.Spec.Containers = append(c.sts.Spec.Template.Spec.Containers, container)
	return c
}

// withReplicas implements statefulSetBuilder.
func (c *componentStatefulSet) withReplicas(replicas int) statefulSetBuilder {
	if replicas > 0 {
		c.sts.Spec.Replicas = ptrInt32(int32(replicas))
	}
	return c
}

// withAffinity implements statefulSetBuilder.
func (c *componentStatefulSet) withAffinity(affinityJSON string) statefulSetBuilder {
	if affinityJSON == "" {
		return c
	}

	var affinity v1.Affinity
	if err := json.Unmarshal([]byte(affinityJSON), &affinity); err != nil {
		return c
	}

	c.sts.Spec.Template.Spec.Affinity = &affinity
	return c
}

// withVolumeClaimTemplate implements statefulSetBuilder.
func (c *componentStatefulSet) withVolumeClaimTemplate(pvc v1.PersistentVolumeClaim) statefulSetBuilder {
	c.sts.Spec.VolumeClaimTemplates = append(c.sts.Spec.VolumeClaimTemplates, pvc)
	return c
}

// build implements statefulSetBuilder.
func (c *componentStatefulSet) build() *appsv1.StatefulSet {
	if c.sts.Spec.Replicas == nil {
		c.sts.Spec.Replicas = ptrInt32(1)
	}
	c.sts.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: c.sts.GetLabels(),
	}
	c.sts.Spec.Template.GetObjectMeta().SetLabels(c.sts.GetLabels())

	return c.sts
}

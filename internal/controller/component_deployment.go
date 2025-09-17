package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type containerBuilder interface {
	withName(name string) containerBuilder
	withImage(image string) containerBuilder
	withVolumeMount(mount v1.VolumeMount) containerBuilder
	withEnv(env []v1.EnvVar) containerBuilder
	build() *v1.Container
}

type componentContainer struct {
	con *v1.Container
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
	c.con.ImagePullPolicy = v1.PullAlways
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

// build implements containerBuilder.
func (c *componentContainer) build() *v1.Container {
	// TODO: see what other common params must be set
	c.con.Resources = v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(1500, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(1536*1024*1024, resource.BinarySI), // 1.5 GB
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(1024*1024*1024, resource.BinarySI), // 1 GB
		},
	}
	// TODO: Make it configurable may be?
	c.con.ImagePullPolicy = v1.PullAlways

	return c.con
}

type deploymentBuilder interface {
	withResourceName(name string) deploymentBuilder
	withNamespace(namespace v1.Namespace) deploymentBuilder
	withLabels(labels map[string]string) deploymentBuilder
	withVolume(volume v1.Volume) deploymentBuilder
	withContainer(container v1.Container) deploymentBuilder
	withReplicas(replicas int) deploymentBuilder
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

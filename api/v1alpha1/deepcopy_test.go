package v1alpha1

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPgBouncerAuroraDeepCopySeparatesMutableFields(t *testing.T) {
	replicas := int32(2)
	now := metav1.Now()
	resource := &PgBouncerAurora{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Labels: map[string]string{"a": "b"}},
		Spec: PgBouncerAuroraSpec{
			PgBouncer: PgBouncerSpec{
				Replicas: &replicas,
				Config: PgBouncerConfigSpec{
					PgBouncer: map[string]string{"pool_mode": "transaction"},
					Databases: map[string]map[string]string{"*": {"user": "svc"}},
				},
				Labels:                   map[string]string{"pod": "label"},
				Annotations:              map[string]string{"pod": "annotation"},
				NodeSelector:             map[string]string{"pool": "db"},
				Affinity:                 &corev1.Affinity{},
				Tolerations:              []corev1.Toleration{{Key: "db"}},
				PodSecurityContext:       &corev1.PodSecurityContext{},
				ContainerSecurityContext: &corev1.SecurityContext{},
				LivenessProbe:            &corev1.Probe{},
				ReadinessProbe:           &corev1.Probe{},
				Sidecars:                 []corev1.Container{{Name: "sidecar", Env: []corev1.EnvVar{{Name: "A", Value: "B"}}}},
				Volumes:                  []corev1.Volume{{Name: "extra"}},
				VolumeMounts:             []corev1.VolumeMount{{Name: "extra", MountPath: "/extra"}},
				InstanceOverrides: []InstanceOverrideSpec{{
					Name: "db-1",
					Config: PgBouncerConfigSpec{
						PgBouncer: map[string]string{"default_pool_size": "100"},
						Databases: map[string]map[string]string{"*": {"pool_size": "100"}},
					},
				}},
			},
			Services: ServicesSpec{
				Writer: ServiceRoleSpec{Annotations: map[string]string{"k": "v"}},
				Reader: ReaderServiceSpec{},
			},
			Monitor: MonitorSpec{DirectDBProbe: boolPtr(true), PgBouncerPathProbe: boolPtr(true)},
			TopologyPolicy: TopologyPolicySpec{
				ZoneAware: ZoneAwareSpec{Enabled: boolPtr(true)},
			},
		},
		Status: PgBouncerAuroraStatus{
			LastMonitorTime:       &now,
			LastAppliedMembership: MembershipStatus{Writer: []string{"db-1"}},
			MissingInstances:      []MissingInstanceStatus{{InstanceName: "db-old", FirstMissingTime: &now, LastMissingTime: &now}},
			Instances:             []InstanceStatus{{InstanceName: "db-1", Healthy: true}},
			Conditions:            []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	resource.Spec.PgBouncer.Resources.Requests = corev1.ResourceList{}

	copied := resource.DeepCopy()
	*copied.Spec.PgBouncer.Replicas = 3
	copied.Spec.PgBouncer.Config.PgBouncer["pool_mode"] = "session"
	copied.Spec.PgBouncer.Config.Databases["*"]["user"] = "other"
	copied.Spec.PgBouncer.Labels["pod"] = "changed"
	copied.Spec.PgBouncer.Annotations["pod"] = "changed"
	copied.Spec.PgBouncer.NodeSelector["pool"] = "other"
	copied.Spec.PgBouncer.Tolerations[0].Key = "other"
	copied.Spec.PgBouncer.Sidecars[0].Env[0].Value = "C"
	copied.Spec.PgBouncer.Volumes[0].Name = "other"
	copied.Spec.PgBouncer.VolumeMounts[0].MountPath = "/other"
	copied.Spec.PgBouncer.InstanceOverrides[0].Config.PgBouncer["default_pool_size"] = "200"
	copied.Spec.PgBouncer.InstanceOverrides[0].Config.Databases["*"]["pool_size"] = "200"
	*copied.Spec.Monitor.DirectDBProbe = false
	*copied.Spec.Monitor.PgBouncerPathProbe = false
	*copied.Spec.TopologyPolicy.ZoneAware.Enabled = false
	copied.Spec.Services.Writer.Annotations["k"] = "changed"
	copied.Status.LastAppliedMembership.Writer[0] = "db-2"
	copied.Status.LastMonitorTime.Time = copied.Status.LastMonitorTime.Time.Add(time.Second)
	copied.Status.MissingInstances[0].FirstMissingTime.Time = copied.Status.MissingInstances[0].FirstMissingTime.Time.Add(time.Second)
	copied.Status.Instances[0].Healthy = false
	copied.Status.Conditions[0].Status = metav1.ConditionFalse

	if *resource.Spec.PgBouncer.Replicas != 2 {
		t.Fatalf("replicas alias detected")
	}
	if resource.Spec.PgBouncer.Config.PgBouncer["pool_mode"] != "transaction" {
		t.Fatalf("config.pgbouncer alias detected")
	}
	if resource.Spec.PgBouncer.Config.Databases["*"]["user"] != "svc" {
		t.Fatalf("config.databases alias detected")
	}
	if resource.Spec.PgBouncer.Labels["pod"] != "label" ||
		resource.Spec.PgBouncer.Annotations["pod"] != "annotation" ||
		resource.Spec.PgBouncer.NodeSelector["pool"] != "db" ||
		resource.Spec.PgBouncer.Tolerations[0].Key != "db" ||
		resource.Spec.PgBouncer.Sidecars[0].Env[0].Value != "B" ||
		resource.Spec.PgBouncer.Volumes[0].Name != "extra" ||
		resource.Spec.PgBouncer.VolumeMounts[0].MountPath != "/extra" {
		t.Fatalf("pod alias detected")
	}
	if resource.Spec.PgBouncer.InstanceOverrides[0].Config.PgBouncer["default_pool_size"] != "100" {
		t.Fatalf("instance override alias detected")
	}
	if resource.Spec.PgBouncer.InstanceOverrides[0].Config.Databases["*"]["pool_size"] != "100" {
		t.Fatalf("instance database override alias detected")
	}
	if resource.Spec.Services.Writer.Annotations["k"] != "v" {
		t.Fatalf("annotation alias detected")
	}
	if !*resource.Spec.Monitor.DirectDBProbe || !*resource.Spec.Monitor.PgBouncerPathProbe || !*resource.Spec.TopologyPolicy.ZoneAware.Enabled {
		t.Fatalf("enabled pointer alias detected")
	}
	if resource.Status.LastAppliedMembership.Writer[0] != "db-1" || !resource.Status.Instances[0].Healthy || resource.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("status alias detected: %#v", resource.Status)
	}
	if resource.Status.MissingInstances[0].FirstMissingTime.Time.Equal(copied.Status.MissingInstances[0].FirstMissingTime.Time) {
		t.Fatalf("missing instance time alias detected")
	}
	if resource.Status.LastMonitorTime.Time.Equal(copied.Status.LastMonitorTime.Time) {
		t.Fatalf("last monitor time alias detected")
	}
}

func boolPtr(value bool) *bool {
	return &value
}

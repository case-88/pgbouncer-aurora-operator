package render

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
)

func TestInstanceDeploymentPreferredZoneAffinity(t *testing.T) {
	replicas := int32(2)
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.Image = "db/pgbouncer:1.25.2"
	owner.Spec.PgBouncer.Replicas = &replicas
	owner.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	owner.Spec.TopologyPolicy.ZoneAware.Enforcement = v1alpha1.ZoneAwarePreferred
	owner.Spec.TopologyPolicy.ZoneAware.TopologyKey = "topology.kubernetes.io/zone"

	deployment := InstanceDeployment(InstanceRenderInput{
		Owner: owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name: "db-1", Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
		}, Replicas: replicas},
	})

	preferred := deployment.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(preferred) != 1 || preferred[0].Preference.MatchExpressions[0].Values[0] != "ap-northeast-2a" {
		t.Fatalf("preferred affinity mismatch: %#v", preferred)
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.ReadinessProbe == nil || container.ReadinessProbe.TCPSocket == nil || container.ReadinessProbe.TCPSocket.Port.IntVal != 6432 {
		t.Fatalf("readiness probe mismatch: %#v", container.ReadinessProbe)
	}
	if container.LivenessProbe == nil || container.LivenessProbe.TCPSocket == nil || container.LivenessProbe.TCPSocket.Port.IntVal != 6432 {
		t.Fatalf("liveness probe mismatch: %#v", container.LivenessProbe)
	}
}

func TestInstanceDeploymentRequiredZoneAffinity(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	owner.Spec.TopologyPolicy.ZoneAware.Enforcement = v1alpha1.ZoneAwareRequired

	deployment := InstanceDeployment(InstanceRenderInput{
		Owner: owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name: "db-1", AvailabilityZone: "ap-northeast-2a",
		}, Replicas: 1},
	})
	required := deployment.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || required.NodeSelectorTerms[0].MatchExpressions[0].Values[0] != "ap-northeast-2a" {
		t.Fatalf("required affinity mismatch: %#v", required)
	}
}

func TestInstanceDeploymentZoneAwareDefaultEnabled(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	deployment := InstanceDeployment(InstanceRenderInput{
		Owner: owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name: "db-1", AvailabilityZone: "ap-northeast-2a",
		}, Replicas: 2},
	})
	if deployment.Spec.Template.Spec.Affinity == nil ||
		deployment.Spec.Template.Spec.Affinity.NodeAffinity == nil ||
		len(deployment.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("zoneAware should default enabled: %#v", deployment.Spec.Template.Spec.Affinity)
	}
}

func TestInstanceDeploymentUsesSafeRollingUpdateStrategy(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	deployment := InstanceDeployment(InstanceRenderInput{
		Owner:    owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 2},
	})
	strategy := deployment.Spec.Strategy
	if strategy.Type != appsv1.RollingUpdateDeploymentStrategyType ||
		strategy.RollingUpdate == nil ||
		strategy.RollingUpdate.MaxUnavailable.IntVal != 0 ||
		strategy.RollingUpdate.MaxSurge.IntVal != 1 {
		t.Fatalf("deployment strategy should be safe rolling update: %#v", strategy)
	}
}

func TestRoleServiceSelector(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.Services.Writer.Name = "writer"
	svc := RoleService(owner, v1alpha1.RoleWriter)
	if svc.Name != "sample-writer" {
		t.Fatalf("service name = %s", svc.Name)
	}
	if svc.Spec.Selector[LabelWriter] != "true" {
		t.Fatalf("selector mismatch: %#v", svc.Spec.Selector)
	}
	if svc.Spec.Ports[0].TargetPort.IntVal != ListenPort(owner.Spec) {
		t.Fatalf("targetPort should be explicitly rendered: %#v", svc.Spec.Ports[0])
	}
	if svc.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		t.Fatalf("protocol should be explicitly rendered: %#v", svc.Spec.Ports[0])
	}
	if svc.Labels[LabelServiceRole] != string(v1alpha1.RoleWriter) {
		t.Fatalf("service role label mismatch: %#v", svc.Labels)
	}
	if svc.Labels[LabelAppName] != AppNameValue ||
		svc.Labels[LabelAppComponent] != AppComponentValue ||
		svc.Labels[LabelAppInstance] != "sample" ||
		svc.Labels[LabelAppManagedBy] != ManagedByValue ||
		svc.Labels[LabelAppPartOf] != AppPartOfValue {
		t.Fatalf("standard app labels missing: %#v", svc.Labels)
	}
}

func TestRoleServiceDefaultName(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	if svc := RoleService(owner, v1alpha1.RoleWriter); svc == nil || svc.Name != "sample-writer" {
		t.Fatalf("writer service should be rendered by default: %#v", svc)
	}
}

func TestResourceNamesAreStableDNSLabels(t *testing.T) {
	clusterName := "sample-pgbouncer-aurora-operator-cluster-name"
	instanceName := "example-aurora-postgresql-instance-identifier-with-long-suffix"

	instanceResourceName := InstanceResourceName(clusterName, instanceName)
	if len(instanceResourceName) > 63 {
		t.Fatalf("instance resource name too long: %s (%d)", instanceResourceName, len(instanceResourceName))
	}
	if instanceResourceName != InstanceResourceName(clusterName, instanceName) {
		t.Fatalf("instance resource name must be stable")
	}
	if instanceResourceName == clusterName+"-"+instanceName {
		t.Fatalf("long instance resource name should be shortened")
	}

	roleServiceName := RoleServiceName(clusterName, "writer")
	if len(roleServiceName) > 63 {
		t.Fatalf("role service name too long: %s (%d)", roleServiceName, len(roleServiceName))
	}
	if roleServiceName != RoleServiceName(clusterName, "writer") {
		t.Fatalf("role service name must be stable")
	}
}

func TestInstanceResourceNameShortensOnlyWhenCombinedNameExceedsDNSLabelLimit(t *testing.T) {
	shortCluster := "pg-poc"
	shortInstance := "pg-poc-3"
	shortName := InstanceResourceName(shortCluster, shortInstance)
	if shortName != "pg-poc-pg-poc-3" {
		t.Fatalf("short name should remain readable, got %s", shortName)
	}

	longInstance := "aurora-instance-name-that-is-valid-but-makes-combined-resource-name-exceed-limit"
	longName := InstanceResourceName(shortCluster, longInstance)
	if len(longName) > 63 {
		t.Fatalf("long resource name too long: %s (%d)", longName, len(longName))
	}
	if longName == shortCluster+"-"+longInstance {
		t.Fatalf("long combined name should be shortened")
	}
	if !strings.HasPrefix(longName, "pg-poc-aurora-instance-name") {
		t.Fatalf("long name should keep readable prefix, got %s", longName)
	}
	if !isDNSLabel(longName) {
		t.Fatalf("long name is not a DNS label: %s", longName)
	}
}

func TestResourceNamesAreNormalizedDNSLabels(t *testing.T) {
	name := RoleServiceName("cluster.with.dot", "Writer_Service")
	if name != "cluster-with-dot-writer-service" {
		t.Fatalf("normalized name = %s", name)
	}
	if !isDNSLabel(name) {
		t.Fatalf("not a DNS label: %s", name)
	}

	long := InstanceResourceName("cluster.with.dot.and-long-name", "Instance_With_Invalid_Chars_And_A_Very_Long_Suffix")
	if len(long) > 63 || !isDNSLabel(long) {
		t.Fatalf("invalid long DNS label: %s (%d)", long, len(long))
	}
}

func TestClusterLabelValueAndAnnotationForLongClusterName(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "cluster.with.dot.and-a-very-long-name-that-exceeds-kubernetes-label-value-limit"
	deployment := InstanceDeployment(InstanceRenderInput{
		Owner:    owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1},
	})
	clusterLabel := deployment.Spec.Template.Labels[LabelCluster]
	if len(clusterLabel) > 63 || !isDNSLabel(clusterLabel) {
		t.Fatalf("invalid cluster label: %s (%d)", clusterLabel, len(clusterLabel))
	}
	if deployment.Spec.Template.Annotations[AnnotationClusterName] != owner.Name {
		t.Fatalf("cluster annotation mismatch: %#v", deployment.Spec.Template.Annotations)
	}
	if deployment.Spec.Selector.MatchLabels[LabelCluster] != clusterLabel {
		t.Fatalf("selector cluster label mismatch: %#v", deployment.Spec.Selector.MatchLabels)
	}
	if deployment.Spec.Template.Labels[LabelAppInstance] != clusterLabel {
		t.Fatalf("app instance label should use safe cluster label: %#v", deployment.Spec.Template.Labels)
	}
}

func TestInstanceDeploymentConfigHashChangesWithPgBouncerConfig(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist"
	owner.Spec.PgBouncer.Config.PgBouncer = map[string]string{"pool_mode": "transaction"}
	owner.Spec.PgBouncer.Config.Databases = map[string]map[string]string{"*": {"user": "svc"}}
	instance := domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example", Port: 5432}, Replicas: 1}

	first := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance})
	firstHash := first.Spec.Template.Annotations[AnnotationConfigHash]
	if firstHash == "" || first.ObjectMeta.Annotations[AnnotationConfigHash] != firstHash {
		t.Fatalf("config hash annotation missing: object=%#v template=%#v", first.ObjectMeta.Annotations, first.Spec.Template.Annotations)
	}

	owner.Spec.PgBouncer.Config.PgBouncer["pool_mode"] = "session"
	second := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance})
	if second.Spec.Template.Annotations[AnnotationConfigHash] == firstHash {
		t.Fatalf("config hash should change when pgbouncer config changes: %s", firstHash)
	}
}

func TestInstanceDeploymentAuthFileHashUsesInputWhenProvided(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist"
	instance := domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1}

	first := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance, AuthFileHash: "secret-content-hash"})
	firstHash := first.Spec.Template.Annotations[AnnotationAuthFileHash]
	if firstHash != "secret-content-hash" || first.ObjectMeta.Annotations[AnnotationAuthFileHash] != firstHash {
		t.Fatalf("auth ref hash annotation missing: object=%#v template=%#v", first.ObjectMeta.Annotations, first.Spec.Template.Annotations)
	}
}

func TestInstanceDeploymentAuthFileHashFallsBackToSecretRef(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist"
	instance := domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1}

	first := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance})
	firstHash := first.Spec.Template.Annotations[AnnotationAuthFileHash]
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist-v2"
	second := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance})
	if firstHash == "" || second.Spec.Template.Annotations[AnnotationAuthFileHash] == firstHash {
		t.Fatalf("auth ref hash should change when auth secret ref changes: %s", firstHash)
	}
}

func TestInstanceDeploymentAuthFileMountPathFollowsConfig(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.Image = "db/pgbouncer:1.25.2"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist"
	owner.Spec.PgBouncer.Config.PgBouncer = map[string]string{"auth_file": "/tmp/userlist.txt"}
	instance := domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1}

	deployment := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: instance})
	mount, ok := volumeMountByName(deployment.Spec.Template.Spec.Containers[0].VolumeMounts, "auth-file")
	if !ok || mount.MountPath != "/tmp/userlist.txt" || mount.SubPath != "userlist.txt" || !mount.ReadOnly {
		t.Fatalf("auth file mount mismatch: %#v", mount)
	}
}

func TestInstanceDeploymentAuthFileMountPathFollowsInstanceOverride(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.Image = "db/pgbouncer:1.25.2"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "pgbouncer-userlist"
	owner.Spec.PgBouncer.Config.PgBouncer = map[string]string{"auth_file": "/base/userlist.txt"}
	owner.Spec.PgBouncer.InstanceOverrides = []v1alpha1.InstanceOverrideSpec{{
		Name: "db-1",
		Config: v1alpha1.PgBouncerConfigSpec{
			PgBouncer: map[string]string{"auth_file": "/override/userlist.txt"},
		},
	}}

	overrideDeployment := InstanceDeployment(InstanceRenderInput{
		Owner:    owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1},
	})
	overrideMount, ok := volumeMountByName(overrideDeployment.Spec.Template.Spec.Containers[0].VolumeMounts, "auth-file")
	if !ok || overrideMount.MountPath != "/override/userlist.txt" {
		t.Fatalf("override auth file mount mismatch: %#v", overrideMount)
	}

	baseDeployment := InstanceDeployment(InstanceRenderInput{
		Owner:    owner,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-2"}, Replicas: 1},
	})
	baseMount, ok := volumeMountByName(baseDeployment.Spec.Template.Spec.Containers[0].VolumeMounts, "auth-file")
	if !ok || baseMount.MountPath != "/base/userlist.txt" {
		t.Fatalf("base auth file mount mismatch: %#v", baseMount)
	}
}

func isDNSLabel(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for i, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			continue
		}
		if char == '-' && i > 0 && i < len(value)-1 {
			continue
		}
		return false
	}
	return true
}

func TestInstanceDeploymentDoesNotSetMembershipLabels(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	deployment := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: domain.RoleWriter}, Replicas: 1}})
	if _, ok := deployment.Spec.Template.Labels[LabelWriter]; ok {
		t.Fatalf("deployment template must not include writer membership label")
	}
	if _, ok := deployment.Spec.Template.Labels[LabelReader]; ok {
		t.Fatalf("deployment template must not include reader membership label")
	}
	if _, ok := deployment.Spec.Template.Labels[LabelRole]; ok {
		t.Fatalf("deployment template must not include dynamic role label")
	}
	if deployment.Labels[LabelRole] != string(domain.RoleWriter) {
		t.Fatalf("deployment object should expose role label: %#v", deployment.Labels)
	}
}

func TestInstanceDeploymentMergesExplicitPodMetadataAndScheduling(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.Image = "db/pgbouncer:1.25.2"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "users"
	owner.Spec.PgBouncer.Labels = map[string]string{
		"custom":       "label",
		LabelInstance:  "must-be-overridden",
		LabelManagedBy: "must-be-overridden",
	}
	owner.Spec.PgBouncer.Annotations = map[string]string{"custom": "annotation"}
	owner.Spec.PgBouncer.NodeSelector = map[string]string{"pool": "db"}
	owner.Spec.PgBouncer.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull-secret"}}
	owner.Spec.PgBouncer.Sidecars = []corev1.Container{{Name: "sidecar", Image: "busybox"}}
	owner.Spec.PgBouncer.Volumes = []corev1.Volume{{Name: "extra"}}
	owner.Spec.PgBouncer.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}
	replicas := int32(2)

	deployment := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: replicas}})
	template := deployment.Spec.Template
	if template.Labels["custom"] != "label" || template.Labels[LabelInstance] != "db-1" || template.Labels[LabelManagedBy] != ManagedByValue {
		t.Fatalf("labels not merged with required override: %#v", template.Labels)
	}
	if template.Annotations["custom"] != "annotation" || template.Annotations[AnnotationClusterName] != "sample" {
		t.Fatalf("annotations not preserved: %#v", template.Annotations)
	}
	if template.Spec.NodeSelector["pool"] != "db" || template.Spec.ImagePullSecrets[0].Name != "pull-secret" {
		t.Fatalf("pod spec scheduling fields not preserved: %#v", template.Spec)
	}
	if len(template.Spec.Containers) != 2 || template.Spec.Containers[0].Name != "pgbouncer" || template.Spec.Containers[1].Name != "sidecar" {
		t.Fatalf("containers not merged: %#v", template.Spec.Containers)
	}
	if !hasVolume(template.Spec.Volumes, "extra") || !hasVolume(template.Spec.Volumes, "config") || !hasVolume(template.Spec.Volumes, "auth-file") {
		t.Fatalf("volumes not merged: %#v", template.Spec.Volumes)
	}
	if len(template.Spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("topology spread constraints should preserve explicit pgbouncer settings: %#v", template.Spec.TopologySpreadConstraints)
	}
}

func TestInstanceDeploymentMergesExplicitPodOptions(t *testing.T) {
	owner := &v1alpha1.PgBouncerAurora{}
	owner.Name = "sample"
	owner.Spec.PgBouncer.Image = "db/pgbouncer:1.25.2"
	owner.Spec.PgBouncer.AuthFileSecretRef.Name = "users"
	runAsNonRoot := true
	readOnlyRootFilesystem := true
	owner.Spec.PgBouncer.Labels = map[string]string{
		"pod":         "label",
		LabelInstance: "must-be-overridden",
	}
	owner.Spec.PgBouncer.Annotations = map[string]string{"pod": "annotation"}
	owner.Spec.PgBouncer.ServiceAccountName = "pgbouncer-sa"
	owner.Spec.PgBouncer.NodeSelector = map[string]string{"pool": "db"}
	owner.Spec.PgBouncer.Tolerations = []corev1.Toleration{{Key: "db", Operator: corev1.TolerationOpExists}}
	owner.Spec.PgBouncer.PriorityClassName = "db-high"
	owner.Spec.PgBouncer.RuntimeClassName = "runc"
	owner.Spec.PgBouncer.PodSecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot}
	owner.Spec.PgBouncer.ContainerSecurityContext = &corev1.SecurityContext{ReadOnlyRootFilesystem: &readOnlyRootFilesystem}
	owner.Spec.PgBouncer.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
	owner.Spec.PgBouncer.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
	owner.Spec.PgBouncer.Sidecars = []corev1.Container{{Name: "sidecar", Image: "busybox"}}
	owner.Spec.PgBouncer.Volumes = []corev1.Volume{{Name: "extra"}}
	owner.Spec.PgBouncer.VolumeMounts = []corev1.VolumeMount{{Name: "extra", MountPath: "/extra"}}

	deployment := InstanceDeployment(InstanceRenderInput{Owner: owner, Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{Name: "db-1"}, Replicas: 1}})
	template := deployment.Spec.Template
	if template.Labels["pod"] != "label" || template.Labels[LabelInstance] != "db-1" {
		t.Fatalf("pod labels not merged with required override: %#v", template.Labels)
	}
	if template.Annotations["pod"] != "annotation" || template.Annotations[AnnotationClusterName] != "sample" {
		t.Fatalf("pod annotations not merged: %#v", template.Annotations)
	}
	if template.Spec.ServiceAccountName != "pgbouncer-sa" ||
		template.Spec.NodeSelector["pool"] != "db" ||
		len(template.Spec.Tolerations) != 1 ||
		template.Spec.PriorityClassName != "db-high" ||
		template.Spec.RuntimeClassName == nil ||
		*template.Spec.RuntimeClassName != "runc" {
		t.Fatalf("pod scheduling options mismatch: %#v", template.Spec)
	}
	if template.Spec.SecurityContext == nil || template.Spec.SecurityContext.RunAsNonRoot == nil || !*template.Spec.SecurityContext.RunAsNonRoot {
		t.Fatalf("pod security context missing: %#v", template.Spec.SecurityContext)
	}
	if len(template.Spec.Containers) != 2 || template.Spec.Containers[0].Name != "pgbouncer" || template.Spec.Containers[1].Name != "sidecar" {
		t.Fatalf("containers mismatch: %#v", template.Spec.Containers)
	}
	main := template.Spec.Containers[0]
	if main.SecurityContext == nil || main.SecurityContext.ReadOnlyRootFilesystem == nil || !*main.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("container security context missing: %#v", main.SecurityContext)
	}
	if main.ReadinessProbe == nil || main.ReadinessProbe.Exec == nil || main.LivenessProbe == nil || main.LivenessProbe.Exec == nil {
		t.Fatalf("custom probes missing: readiness=%#v liveness=%#v", main.ReadinessProbe, main.LivenessProbe)
	}
	if !hasVolume(template.Spec.Volumes, "extra") || !hasVolume(template.Spec.Volumes, "config") || !hasVolumeMount(main.VolumeMounts, "extra") || !hasVolumeMount(main.VolumeMounts, "auth-file") {
		t.Fatalf("volumes or mounts missing: volumes=%#v mounts=%#v", template.Spec.Volumes, main.VolumeMounts)
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func hasVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func volumeMountByName(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, mount := range mounts {
		if mount.Name == name {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
}

func boolPtr(value bool) *bool {
	return &value
}

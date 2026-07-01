package render

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
)

const (
	LabelManagedBy          = "pgbouncer-aurora.io/managed-by"
	LabelCluster            = "pgbouncer-aurora.io/cluster"
	LabelInstance           = "pgbouncer-aurora.io/instance"
	LabelRole               = "pgbouncer-aurora.io/role"
	LabelServiceRole        = "pgbouncer-aurora.io/service-role"
	LabelWriter             = "pgbouncer-aurora.io/member-writer"
	LabelReader             = "pgbouncer-aurora.io/member-reader"
	LabelAppName            = "app.kubernetes.io/name"
	LabelAppComponent       = "app.kubernetes.io/component"
	LabelAppInstance        = "app.kubernetes.io/instance"
	LabelAppManagedBy       = "app.kubernetes.io/managed-by"
	LabelAppPartOf          = "app.kubernetes.io/part-of"
	AnnotationClusterName   = "pgbouncer-aurora.io/cluster-name"
	AnnotationConfigHash    = "pgbouncer-aurora.io/config-hash"
	AnnotationAuthFileHash  = "pgbouncer-aurora.io/auth-file-hash"
	AnnotationDbiResourceID = "pgbouncer-aurora.io/dbi-resource-id"
	ManagedByValue          = "pgbouncer-aurora-operator"
	AppNameValue            = "pgbouncer-aurora"
	AppComponentValue       = "pgbouncer"
	AppPartOfValue          = "pgbouncer-aurora"

	maxDNSLabelLength = 63
	nameHashLength    = 10
)

type InstanceRenderInput struct {
	Owner        *v1alpha1.PgBouncerAurora
	Instance     domain.InstancePlan
	AuthFileHash string
}

func InstanceDeployment(input InstanceRenderInput) *appsv1.Deployment {
	owner := input.Owner
	instance := input.Instance
	replicas := instance.Replicas
	listenPort := ListenPort(owner.Spec)
	labels := mergeMap(owner.Spec.PgBouncer.Labels, baseLabels(owner.Name, instance.Name))
	deploymentLabels := cloneMap(labels)
	deploymentLabels[LabelRole] = string(instance.Role)
	annotations := mergeMap(owner.Spec.PgBouncer.Annotations, deploymentAnnotations(owner, instance.InstanceObservation, input.AuthFileHash))
	selector := baseLabels(owner.Name, instance.Name)
	podSpec := podSpec(owner, instance.Name, listenPort)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        instanceResourceName(owner.Name, instance.Name),
			Namespace:   owner.Namespace,
			Labels:      deploymentLabels,
			Annotations: deploymentAnnotations(owner, instance.InstanceObservation, input.AuthFileHash),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: rollingUpdateStrategy(),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       podSpec,
			},
		},
	}
	applyZoneAffinity(deployment, owner.Spec.TopologyPolicy.ZoneAware, instance.AvailabilityZone)
	return deployment
}

func rollingUpdateStrategy() appsv1.DeploymentStrategy {
	return appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxUnavailable: intstrPtr(0),
			MaxSurge:       intstrPtr(1),
		},
	}
}

func intstrPtr(value int) *intstr.IntOrString {
	out := intstr.FromInt(value)
	return &out
}

func podSpec(owner *v1alpha1.PgBouncerAurora, instanceName string, listenPort int32) corev1.PodSpec {
	spec := corev1.PodSpec{}
	pgbouncer := owner.Spec.PgBouncer
	if pgbouncer.ServiceAccountName != "" {
		spec.ServiceAccountName = pgbouncer.ServiceAccountName
	}
	spec.NodeSelector = mergeMap(spec.NodeSelector, pgbouncer.NodeSelector)
	if pgbouncer.Affinity != nil {
		spec.Affinity = pgbouncer.Affinity.DeepCopy()
	}
	if len(pgbouncer.Tolerations) > 0 {
		spec.Tolerations = append(spec.Tolerations, cloneTolerations(pgbouncer.Tolerations)...)
	}
	if len(pgbouncer.ImagePullSecrets) > 0 {
		spec.ImagePullSecrets = append(spec.ImagePullSecrets, pgbouncer.ImagePullSecrets...)
	}
	if len(pgbouncer.TopologySpreadConstraints) > 0 {
		spec.TopologySpreadConstraints = append(spec.TopologySpreadConstraints, cloneTopologySpreadConstraints(pgbouncer.TopologySpreadConstraints)...)
	}
	if pgbouncer.PriorityClassName != "" {
		spec.PriorityClassName = pgbouncer.PriorityClassName
	}
	if pgbouncer.RuntimeClassName != "" {
		spec.RuntimeClassName = stringPtr(pgbouncer.RuntimeClassName)
	}
	if pgbouncer.PodSecurityContext != nil {
		spec.SecurityContext = pgbouncer.PodSecurityContext.DeepCopy()
	}
	if len(pgbouncer.Sidecars) > 0 {
		spec.Containers = append(spec.Containers, cloneContainers(pgbouncer.Sidecars)...)
	}
	if len(pgbouncer.Volumes) > 0 {
		spec.Volumes = append(spec.Volumes, cloneVolumes(pgbouncer.Volumes)...)
	}
	spec.RestartPolicy = corev1.RestartPolicyAlways
	spec.Containers = mergeContainers(spec.Containers, pgbouncerContainer(owner, listenPort, instanceName))
	spec.Volumes = mergeVolumes(spec.Volumes, requiredVolumes(owner, instanceName))
	return spec
}

func pgbouncerContainer(owner *v1alpha1.PgBouncerAurora, listenPort int32, instanceName string) corev1.Container {
	pgbouncer := owner.Spec.PgBouncer
	readinessProbe := tcpProbe(listenPort)
	if probeConfigured(pgbouncer.ReadinessProbe) {
		readinessProbe = pgbouncer.ReadinessProbe.DeepCopy()
	}
	livenessProbe := tcpProbe(listenPort)
	if probeConfigured(pgbouncer.LivenessProbe) {
		livenessProbe = pgbouncer.LivenessProbe.DeepCopy()
	}
	volumeMounts := mergeVolumeMounts(pgbouncer.VolumeMounts, []corev1.VolumeMount{
		{Name: "config", MountPath: "/etc/pgbouncer/pgbouncer.ini", SubPath: "pgbouncer.ini", ReadOnly: true},
		{Name: "auth-file", MountPath: AuthFilePath(owner.Spec, instanceName), SubPath: "userlist.txt", ReadOnly: true},
	})
	return corev1.Container{
		Name:            "pgbouncer",
		Image:           owner.Spec.PgBouncer.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{{
			Name:          "pgbouncer",
			ContainerPort: listenPort,
		}},
		ReadinessProbe:  readinessProbe,
		LivenessProbe:   livenessProbe,
		Resources:       owner.Spec.PgBouncer.Resources,
		SecurityContext: pgbouncer.ContainerSecurityContext.DeepCopy(),
		VolumeMounts:    volumeMounts,
	}
}

func requiredVolumes(owner *v1alpha1.PgBouncerAurora, instanceName string) []corev1.Volume {
	return []corev1.Volume{
		{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: instanceResourceName(owner.Name, instanceName)}}}},
		{Name: "auth-file", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: owner.Spec.PgBouncer.AuthFileSecretRef.Name}}},
	}
}

func tcpProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(port))},
		},
		InitialDelaySeconds: 5,
		TimeoutSeconds:      3,
		PeriodSeconds:       10,
	}
}

func probeConfigured(probe *corev1.Probe) bool {
	if probe == nil {
		return false
	}
	return probe.Exec != nil || probe.HTTPGet != nil || probe.TCPSocket != nil || probe.GRPC != nil
}

func InstanceService(owner *v1alpha1.PgBouncerAurora, instance domain.InstancePlan) *corev1.Service {
	serviceType := owner.Spec.Services.PerInstances.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	listenPort := ListenPort(owner.Spec)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        instanceResourceName(owner.Name, instance.Name),
			Namespace:   owner.Namespace,
			Labels:      baseLabels(owner.Name, instance.Name),
			Annotations: mergeMap(owner.Spec.Services.PerInstances.Annotations, clusterAnnotations(owner.Name)),
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: baseLabels(owner.Name, instance.Name),
			Ports: []corev1.ServicePort{{
				Name:       "pgbouncer",
				Protocol:   corev1.ProtocolTCP,
				Port:       listenPort,
				TargetPort: intstr.FromInt(int(listenPort)),
			}},
		},
	}
}

func RoleService(owner *v1alpha1.PgBouncerAurora, role v1alpha1.Role) *corev1.Service {
	spec := owner.Spec.Services.Writer
	selectorKey := LabelWriter
	defaultName := "writer"
	if role == v1alpha1.RoleReader {
		spec = owner.Spec.Services.Reader.ServiceRoleSpec
		selectorKey = LabelReader
		defaultName = "reader"
	}
	serviceType := spec.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	name := spec.Name
	if name == "" {
		name = defaultName
	}
	listenPort := ListenPort(owner.Spec)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        RoleServiceName(owner.Name, name),
			Namespace:   owner.Namespace,
			Labels:      mergeMap(baseClusterLabels(owner.Name), map[string]string{LabelServiceRole: string(role)}),
			Annotations: mergeMap(spec.Annotations, clusterAnnotations(owner.Name)),
		},
		Spec: corev1.ServiceSpec{
			Type: serviceType,
			Selector: map[string]string{
				LabelManagedBy: ManagedByValue,
				LabelCluster:   ClusterLabelValue(owner.Name),
				selectorKey:    "true",
			},
			Ports: []corev1.ServicePort{{
				Name:       "pgbouncer",
				Protocol:   corev1.ProtocolTCP,
				Port:       listenPort,
				TargetPort: intstr.FromInt(int(listenPort)),
			}},
		},
	}
}

func ConfigMap(owner *v1alpha1.PgBouncerAurora, instance domain.InstanceObservation) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        instanceResourceName(owner.Name, instance.Name),
			Namespace:   owner.Namespace,
			Labels:      baseLabels(owner.Name, instance.Name),
			Annotations: clusterAnnotations(owner.Name),
		},
		Data: map[string]string{
			"pgbouncer.ini": PgBouncerINI(owner.Spec, instance),
		},
	}
}

func applyZoneAffinity(deployment *appsv1.Deployment, spec v1alpha1.ZoneAwareSpec, zone string) {
	if !enabledDefaultTrue(spec.Enabled) || zone == "" {
		return
	}
	topologyKey := firstNonEmpty(spec.TopologyKey, "topology.kubernetes.io/zone")
	requirement := corev1.NodeSelectorRequirement{Key: topologyKey, Operator: corev1.NodeSelectorOpIn, Values: []string{zone}}
	if deployment.Spec.Template.Spec.Affinity == nil {
		deployment.Spec.Template.Spec.Affinity = &corev1.Affinity{}
	}
	if deployment.Spec.Template.Spec.Affinity.NodeAffinity == nil {
		deployment.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	nodeAffinity := deployment.Spec.Template.Spec.Affinity.NodeAffinity
	if spec.Enforcement == v1alpha1.ZoneAwareRequired {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = mergeRequiredNodeAffinity(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, requirement)
		return
	}
	nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, corev1.PreferredSchedulingTerm{
		Weight:     100,
		Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{requirement}},
	})
}

func mergeRequiredNodeAffinity(existing *corev1.NodeSelector, requirement corev1.NodeSelectorRequirement) *corev1.NodeSelector {
	if existing == nil || len(existing.NodeSelectorTerms) == 0 {
		return &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{requirement}}}}
	}
	out := existing.DeepCopy()
	for i := range out.NodeSelectorTerms {
		out.NodeSelectorTerms[i].MatchExpressions = append(out.NodeSelectorTerms[i].MatchExpressions, requirement)
	}
	return out
}

func baseLabels(clusterName, instanceName string) map[string]string {
	labels := baseClusterLabels(clusterName)
	labels[LabelInstance] = instanceName
	return labels
}

func baseClusterLabels(clusterName string) map[string]string {
	return map[string]string{
		LabelManagedBy:    ManagedByValue,
		LabelCluster:      ClusterLabelValue(clusterName),
		LabelAppName:      AppNameValue,
		LabelAppComponent: AppComponentValue,
		LabelAppInstance:  ClusterLabelValue(clusterName),
		LabelAppManagedBy: ManagedByValue,
		LabelAppPartOf:    AppPartOfValue,
	}
}

func clusterAnnotations(clusterName string) map[string]string {
	return map[string]string{AnnotationClusterName: clusterName}
}

func deploymentAnnotations(owner *v1alpha1.PgBouncerAurora, instance domain.InstanceObservation, authFileHash string) map[string]string {
	annotations := clusterAnnotations(owner.Name)
	annotations[AnnotationConfigHash] = hashString(PgBouncerINI(owner.Spec, instance))
	if authFileHash == "" {
		authFileHash = hashString(owner.Spec.PgBouncer.AuthFileSecretRef.Name)
	}
	annotations[AnnotationAuthFileHash] = authFileHash
	if strings.TrimSpace(instance.DbiResourceId) != "" {
		annotations[AnnotationDbiResourceID] = strings.TrimSpace(instance.DbiResourceId)
	}
	return annotations
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func ClusterLabelValue(clusterName string) string {
	return safeDNSLabel(clusterName)
}

func instanceResourceName(clusterName, instanceName string) string {
	return safeDNSLabel(clusterName, instanceName)
}

func InstanceResourceName(clusterName, instanceName string) string {
	return instanceResourceName(clusterName, instanceName)
}

func RoleServiceName(clusterName, serviceName string) string {
	return safeDNSLabel(clusterName, serviceName)
}

func safeDNSLabel(parts ...string) string {
	raw := strings.Join(parts, "-")
	normalized := normalizeDNSLabel(raw)
	if len(normalized) <= maxDNSLabelLength {
		return normalized
	}
	sum := sha1.Sum([]byte(raw))
	suffix := hex.EncodeToString(sum[:])[:nameHashLength]
	prefixLimit := maxDNSLabelLength - nameHashLength - 1
	prefix := strings.TrimRight(normalized[:prefixLimit], "-")
	if prefix == "" {
		prefix = "x"
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func normalizeDNSLabel(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	lastDash := false
	for _, char := range value {
		allowed := (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')
		if allowed {
			builder.WriteRune(char)
			lastDash = false
			continue
		}
		if char == '-' || !allowed {
			if !lastDash {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(builder.String(), "-")
	if out == "" {
		return "x"
	}
	return out
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func stringPtr(value string) *string {
	return &value
}

func cloneTolerations(in []corev1.Toleration) []corev1.Toleration {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.Toleration, len(in))
	for i := range in {
		out[i] = *in[i].DeepCopy()
	}
	return out
}

func cloneContainers(in []corev1.Container) []corev1.Container {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.Container, len(in))
	for i := range in {
		out[i] = *in[i].DeepCopy()
	}
	return out
}

func cloneTopologySpreadConstraints(in []corev1.TopologySpreadConstraint) []corev1.TopologySpreadConstraint {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.TopologySpreadConstraint, len(in))
	for i := range in {
		out[i] = *in[i].DeepCopy()
	}
	return out
}

func cloneVolumes(in []corev1.Volume) []corev1.Volume {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.Volume, len(in))
	for i := range in {
		out[i] = *in[i].DeepCopy()
	}
	return out
}

func enabledDefaultTrue(value *bool) bool {
	return value == nil || *value
}

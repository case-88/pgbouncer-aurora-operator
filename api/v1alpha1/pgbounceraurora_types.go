package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Role string

const (
	RoleWriter Role = "writer"
	RoleReader Role = "reader"
)

type PgBouncerAuroraSpec struct {
	// +kubebuilder:validation:Required
	Discovery DiscoverySpec `json:"discovery,omitempty"`
	Monitor   MonitorSpec   `json:"monitor,omitempty"`
	// +kubebuilder:validation:Required
	PgBouncer      PgBouncerSpec      `json:"pgbouncer,omitempty"`
	Services       ServicesSpec       `json:"services,omitempty"`
	TopologyPolicy TopologyPolicySpec `json:"topologyPolicy,omitempty"`
}

type DiscoverySpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClusterName string `json:"clusterName,omitempty"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	DomainName string `json:"domainName,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port             int32                         `json:"port,omitempty"`
	ClusterEndpoints DiscoveryClusterEndpointsSpec `json:"clusterEndpoints,omitempty"`
	Database         string                        `json:"database,omitempty"`
	// +kubebuilder:validation:Required
	AuthSecretRef           corev1.LocalObjectReference `json:"authSecretRef,omitempty"`
	Interval                metav1.Duration             `json:"interval,omitempty"`
	MetadataRefreshInterval metav1.Duration             `json:"metadataRefreshInterval,omitempty"`
	Timeout                 metav1.Duration             `json:"timeout,omitempty"`
	// +kubebuilder:validation:Minimum=1
	FailureThreshold int32 `json:"failureThreshold,omitempty"`
}

type DiscoveryClusterEndpointsSpec struct {
	Writer DiscoveryEndpointSpec `json:"writer,omitempty"`
	Reader DiscoveryEndpointSpec `json:"reader,omitempty"`
}

type DiscoveryEndpointSpec struct {
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`
}

type MonitorSpec struct {
	Interval           metav1.Duration `json:"interval,omitempty"`
	Timeout            metav1.Duration `json:"timeout,omitempty"`
	FailureThreshold   int32           `json:"failureThreshold,omitempty"`
	RecoveryThreshold  int32           `json:"recoveryThreshold,omitempty"`
	MaxConcurrency     int32           `json:"maxConcurrency,omitempty"`
	DirectDBProbe      *bool           `json:"directDBProbe,omitempty"`
	PgBouncerPathProbe *bool           `json:"pgbouncerPathProbe,omitempty"`
	DirectDBSSLMode    string          `json:"directDBSSLMode,omitempty"`
}

type PgBouncerSpec struct {
	// +kubebuilder:validation:MinLength=1
	Image             string                 `json:"image,omitempty"`
	Replicas          *int32                 `json:"replicas,omitempty"`
	Config            PgBouncerConfigSpec    `json:"config,omitempty"`
	InstanceOverrides []InstanceOverrideSpec `json:"instanceOverrides,omitempty"`
	// +kubebuilder:validation:Required
	AuthFileSecretRef         corev1.LocalObjectReference       `json:"authFileSecretRef,omitempty"`
	Labels                    map[string]string                 `json:"labels,omitempty"`
	Annotations               map[string]string                 `json:"annotations,omitempty"`
	Resources                 corev1.ResourceRequirements       `json:"resources,omitempty"`
	ServiceAccountName        string                            `json:"serviceAccountName,omitempty"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
	RuntimeClassName          string                            `json:"runtimeClassName,omitempty"`
	PodSecurityContext        *corev1.PodSecurityContext        `json:"podSecurityContext,omitempty"`
	ContainerSecurityContext  *corev1.SecurityContext           `json:"containerSecurityContext,omitempty"`
	LivenessProbe             *corev1.Probe                     `json:"livenessProbe,omitempty"`
	ReadinessProbe            *corev1.Probe                     `json:"readinessProbe,omitempty"`
	Sidecars                  []corev1.Container                `json:"sidecars,omitempty"`
	Volumes                   []corev1.Volume                   `json:"volumes,omitempty"`
	VolumeMounts              []corev1.VolumeMount              `json:"volumeMounts,omitempty"`
	ImagePullSecrets          []corev1.LocalObjectReference     `json:"imagePullSecrets,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

type PgBouncerConfigSpec struct {
	PgBouncer map[string]string            `json:"pgbouncer,omitempty"`
	Databases map[string]map[string]string `json:"databases,omitempty"`
	Users     map[string]map[string]string `json:"users,omitempty"`
	Peers     map[string]map[string]string `json:"peers,omitempty"`
}

type InstanceOverrideSpec struct {
	// +kubebuilder:validation:MinLength=1
	Name     string              `json:"name,omitempty"`
	Replicas int32               `json:"replicas,omitempty"`
	Config   PgBouncerConfigSpec `json:"config,omitempty"`
}

type ServicesSpec struct {
	Writer       ServiceRoleSpec        `json:"writer,omitempty"`
	Reader       ReaderServiceSpec      `json:"reader,omitempty"`
	PerInstances PerInstanceServiceSpec `json:"perInstances,omitempty"`
}

type ServiceRoleSpec struct {
	Name        string             `json:"name,omitempty"`
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

type ReaderServiceSpec struct {
	ServiceRoleSpec `json:",inline"`
}

type PerInstanceServiceSpec struct {
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

type TopologyPolicySpec struct {
	RemoveAfterMissingCount        int32                          `json:"removeAfterMissingCount,omitempty"`
	RemovedInstanceRetention       metav1.Duration                `json:"removedInstanceRetention,omitempty"`
	WriterChangeConnectionHandling WriterChangeConnectionHandling `json:"writerChangeConnectionHandling,omitempty"`
	ReaderEmptyFallback            ReaderEmptyFallbackSpec        `json:"readerEmptyFallback,omitempty"`
	ZoneAware                      ZoneAwareSpec                  `json:"zoneAware,omitempty"`
}

type ReaderEmptyFallbackSpec struct {
	Enabled *bool `json:"enabled,omitempty"`
}

type WriterChangeConnectionHandling string

const (
	WriterChangeKeepExisting   WriterChangeConnectionHandling = "KeepExisting"
	WriterChangeRestartWriters WriterChangeConnectionHandling = "RestartWriters"
	WriterChangeRestartAll     WriterChangeConnectionHandling = "RestartAll"
)

type ZoneAwareSpec struct {
	Enabled        *bool                   `json:"enabled,omitempty"`
	Enforcement    ZoneAwareEnforcement    `json:"enforcement,omitempty"`
	TopologyKey    string                  `json:"topologyKey,omitempty"`
	ConflictPolicy ZoneAwareConflictPolicy `json:"conflictPolicy,omitempty"`
}

type ZoneAwareEnforcement string

const (
	ZoneAwarePreferred ZoneAwareEnforcement = "Preferred"
	ZoneAwareRequired  ZoneAwareEnforcement = "Required"
)

type ZoneAwareConflictPolicy string

const (
	ZoneAwareConflictWarn   ZoneAwareConflictPolicy = "Warn"
	ZoneAwareConflictFail   ZoneAwareConflictPolicy = "Fail"
	ZoneAwareConflictIgnore ZoneAwareConflictPolicy = "Ignore"
)

type PgBouncerAuroraStatus struct {
	ObservedGeneration           int64                   `json:"observedGeneration,omitempty"`
	TopologyHash                 string                  `json:"topologyHash,omitempty"`
	MembershipHash               string                  `json:"membershipHash,omitempty"`
	ConsecutiveDiscoveryFailures int32                   `json:"consecutiveDiscoveryFailures,omitempty"`
	LastDiscoveryTime            *metav1.Time            `json:"lastDiscoveryTime,omitempty"`
	LastMonitorTime              *metav1.Time            `json:"lastMonitorTime,omitempty"`
	LastAppliedTime              *metav1.Time            `json:"lastAppliedTime,omitempty"`
	LastKnownTopology            TopologyStatus          `json:"lastKnownTopology,omitempty"`
	LastAppliedMembership        MembershipStatus        `json:"lastAppliedMembership,omitempty"`
	ServiceSummary               ServiceSummaryStatus    `json:"serviceSummary,omitempty"`
	MissingInstances             []MissingInstanceStatus `json:"missingInstances,omitempty"`
	Instances                    []InstanceStatus        `json:"instances,omitempty"`
	Conditions                   []metav1.Condition      `json:"conditions,omitempty"`
}

type TopologyStatus struct {
	Instances []InstanceTopologyStatus `json:"instances,omitempty"`
}

type InstanceTopologyStatus struct {
	InstanceName     string `json:"instanceName,omitempty"`
	Endpoint         string `json:"endpoint,omitempty"`
	Port             int32  `json:"port,omitempty"`
	Role             Role   `json:"role,omitempty"`
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	DbiResourceId    string `json:"dbiResourceId,omitempty"`
}

type MembershipStatus struct {
	Writer []string `json:"writer,omitempty"`
	Reader []string `json:"reader,omitempty"`
}

type MissingInstanceStatus struct {
	InstanceName     string       `json:"instanceName,omitempty"`
	MissingCount     int32        `json:"missingCount,omitempty"`
	FirstMissingTime *metav1.Time `json:"firstMissingTime,omitempty"`
	LastMissingTime  *metav1.Time `json:"lastMissingTime,omitempty"`
}

type ServiceSummaryStatus struct {
	Writer RoleServiceSummaryStatus `json:"writer,omitempty"`
	Reader RoleServiceSummaryStatus `json:"reader,omitempty"`
}

type RoleServiceSummaryStatus struct {
	ServiceName        string `json:"serviceName,omitempty"`
	DesiredRole        Role   `json:"desiredRole,omitempty"`
	TotalCandidates    int32  `json:"totalCandidates,omitempty"`
	Healthy            int32  `json:"healthy,omitempty"`
	Unhealthy          int32  `json:"unhealthy,omitempty"`
	Members            int32  `json:"members,omitempty"`
	ReadyMembers       int32  `json:"readyMembers,omitempty"`
	FallbackFromWriter bool   `json:"fallbackFromWriter,omitempty"`
}

type InstanceStatus struct {
	InstanceName         string `json:"instanceName,omitempty"`
	Endpoint             string `json:"endpoint,omitempty"`
	Port                 int32  `json:"port,omitempty"`
	Role                 Role   `json:"role,omitempty"`
	AvailabilityZone     string `json:"availabilityZone,omitempty"`
	DbiResourceId        string `json:"dbiResourceId,omitempty"`
	Healthy              bool   `json:"healthy,omitempty"`
	DesiredReplicas      int32  `json:"desiredReplicas,omitempty"`
	ReadyReplicas        int32  `json:"readyReplicas,omitempty"`
	ConsecutiveFailures  int32  `json:"consecutiveFailures,omitempty"`
	ConsecutiveSuccesses int32  `json:"consecutiveSuccesses,omitempty"`
	Reason               string `json:"reason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type PgBouncerAurora struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgBouncerAuroraSpec   `json:"spec,omitempty"`
	Status PgBouncerAuroraStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PgBouncerAuroraList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgBouncerAurora `json:"items"`
}

func (in *PgBouncerAurora) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *PgBouncerAuroraList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *PgBouncerAurora) DeepCopy() *PgBouncerAurora {
	if in == nil {
		return nil
	}
	out := new(PgBouncerAurora)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = deepCopySpec(in.Spec)
	out.Status = deepCopyStatus(in.Status)
	return out
}

func (in *PgBouncerAuroraList) DeepCopy() *PgBouncerAuroraList {
	if in == nil {
		return nil
	}
	out := new(PgBouncerAuroraList)
	*out = *in
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]PgBouncerAurora, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func deepCopySpec(in PgBouncerAuroraSpec) PgBouncerAuroraSpec {
	out := in
	if in.PgBouncer.Replicas != nil {
		out.PgBouncer.Replicas = new(int32)
		*out.PgBouncer.Replicas = *in.PgBouncer.Replicas
	}
	out.Monitor.DirectDBProbe = cloneBoolPtr(in.Monitor.DirectDBProbe)
	out.Monitor.PgBouncerPathProbe = cloneBoolPtr(in.Monitor.PgBouncerPathProbe)
	out.PgBouncer.Config = deepCopyPgBouncerConfigSpec(in.PgBouncer.Config)
	if in.PgBouncer.InstanceOverrides != nil {
		out.PgBouncer.InstanceOverrides = make([]InstanceOverrideSpec, len(in.PgBouncer.InstanceOverrides))
		for i := range in.PgBouncer.InstanceOverrides {
			out.PgBouncer.InstanceOverrides[i] = in.PgBouncer.InstanceOverrides[i]
			out.PgBouncer.InstanceOverrides[i].Config = deepCopyPgBouncerConfigSpec(in.PgBouncer.InstanceOverrides[i].Config)
		}
	}
	out.PgBouncer.Resources = *in.PgBouncer.Resources.DeepCopy()
	out.PgBouncer.Labels = cloneStringMap(in.PgBouncer.Labels)
	out.PgBouncer.Annotations = cloneStringMap(in.PgBouncer.Annotations)
	out.PgBouncer.NodeSelector = cloneStringMap(in.PgBouncer.NodeSelector)
	if in.PgBouncer.Affinity != nil {
		out.PgBouncer.Affinity = in.PgBouncer.Affinity.DeepCopy()
	}
	if in.PgBouncer.Tolerations != nil {
		out.PgBouncer.Tolerations = make([]corev1.Toleration, len(in.PgBouncer.Tolerations))
		for i := range in.PgBouncer.Tolerations {
			out.PgBouncer.Tolerations[i] = *in.PgBouncer.Tolerations[i].DeepCopy()
		}
	}
	if in.PgBouncer.PodSecurityContext != nil {
		out.PgBouncer.PodSecurityContext = in.PgBouncer.PodSecurityContext.DeepCopy()
	}
	if in.PgBouncer.ContainerSecurityContext != nil {
		out.PgBouncer.ContainerSecurityContext = in.PgBouncer.ContainerSecurityContext.DeepCopy()
	}
	if in.PgBouncer.LivenessProbe != nil {
		out.PgBouncer.LivenessProbe = in.PgBouncer.LivenessProbe.DeepCopy()
	}
	if in.PgBouncer.ReadinessProbe != nil {
		out.PgBouncer.ReadinessProbe = in.PgBouncer.ReadinessProbe.DeepCopy()
	}
	if in.PgBouncer.Sidecars != nil {
		out.PgBouncer.Sidecars = make([]corev1.Container, len(in.PgBouncer.Sidecars))
		for i := range in.PgBouncer.Sidecars {
			out.PgBouncer.Sidecars[i] = *in.PgBouncer.Sidecars[i].DeepCopy()
		}
	}
	if in.PgBouncer.Volumes != nil {
		out.PgBouncer.Volumes = make([]corev1.Volume, len(in.PgBouncer.Volumes))
		for i := range in.PgBouncer.Volumes {
			out.PgBouncer.Volumes[i] = *in.PgBouncer.Volumes[i].DeepCopy()
		}
	}
	if in.PgBouncer.VolumeMounts != nil {
		out.PgBouncer.VolumeMounts = make([]corev1.VolumeMount, len(in.PgBouncer.VolumeMounts))
		for i := range in.PgBouncer.VolumeMounts {
			out.PgBouncer.VolumeMounts[i] = *in.PgBouncer.VolumeMounts[i].DeepCopy()
		}
	}
	if in.PgBouncer.ImagePullSecrets != nil {
		out.PgBouncer.ImagePullSecrets = make([]corev1.LocalObjectReference, len(in.PgBouncer.ImagePullSecrets))
		copy(out.PgBouncer.ImagePullSecrets, in.PgBouncer.ImagePullSecrets)
	}
	if in.PgBouncer.TopologySpreadConstraints != nil {
		out.PgBouncer.TopologySpreadConstraints = make([]corev1.TopologySpreadConstraint, len(in.PgBouncer.TopologySpreadConstraints))
		for i := range in.PgBouncer.TopologySpreadConstraints {
			out.PgBouncer.TopologySpreadConstraints[i] = *in.PgBouncer.TopologySpreadConstraints[i].DeepCopy()
		}
	}
	out.Services.Writer.Annotations = cloneStringMap(in.Services.Writer.Annotations)
	out.Services.Reader.ServiceRoleSpec.Annotations = cloneStringMap(in.Services.Reader.ServiceRoleSpec.Annotations)
	out.Services.PerInstances.Annotations = cloneStringMap(in.Services.PerInstances.Annotations)
	out.TopologyPolicy.ReaderEmptyFallback.Enabled = cloneBoolPtr(in.TopologyPolicy.ReaderEmptyFallback.Enabled)
	out.TopologyPolicy.ZoneAware.Enabled = cloneBoolPtr(in.TopologyPolicy.ZoneAware.Enabled)
	return out
}

func deepCopyPgBouncerConfigSpec(in PgBouncerConfigSpec) PgBouncerConfigSpec {
	out := in
	out.PgBouncer = cloneStringMap(in.PgBouncer)
	out.Databases = cloneStringMapMap(in.Databases)
	out.Users = cloneStringMapMap(in.Users)
	out.Peers = cloneStringMapMap(in.Peers)
	return out
}

func deepCopyStatus(in PgBouncerAuroraStatus) PgBouncerAuroraStatus {
	out := in
	if in.LastDiscoveryTime != nil {
		out.LastDiscoveryTime = in.LastDiscoveryTime.DeepCopy()
	}
	if in.LastMonitorTime != nil {
		out.LastMonitorTime = in.LastMonitorTime.DeepCopy()
	}
	if in.LastAppliedTime != nil {
		out.LastAppliedTime = in.LastAppliedTime.DeepCopy()
	}
	out.LastKnownTopology.Instances = cloneTopologyStatuses(in.LastKnownTopology.Instances)
	out.LastAppliedMembership.Writer = cloneStringSlice(in.LastAppliedMembership.Writer)
	out.LastAppliedMembership.Reader = cloneStringSlice(in.LastAppliedMembership.Reader)
	out.MissingInstances = cloneMissingInstanceStatuses(in.MissingInstances)
	out.Instances = cloneInstanceStatuses(in.Instances)
	out.Conditions = cloneConditions(in.Conditions)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStringMapMap(in map[string]map[string]string) map[string]map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[string]string, len(in))
	for key, value := range in {
		out[key] = cloneStringMap(value)
	}
	return out
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneBoolPtr(in *bool) *bool {
	if in == nil {
		return nil
	}
	out := new(bool)
	*out = *in
	return out
}

func cloneTopologyStatuses(in []InstanceTopologyStatus) []InstanceTopologyStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]InstanceTopologyStatus, len(in))
	copy(out, in)
	return out
}

func cloneInstanceStatuses(in []InstanceStatus) []InstanceStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]InstanceStatus, len(in))
	copy(out, in)
	return out
}

func cloneMissingInstanceStatuses(in []MissingInstanceStatus) []MissingInstanceStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]MissingInstanceStatus, len(in))
	for i := range in {
		out[i] = in[i]
		if in[i].FirstMissingTime != nil {
			out[i].FirstMissingTime = in[i].FirstMissingTime.DeepCopy()
		}
		if in[i].LastMissingTime != nil {
			out[i].LastMissingTime = in[i].LastMissingTime.DeepCopy()
		}
	}
	return out
}

func cloneConditions(in []metav1.Condition) []metav1.Condition {
	if len(in) == 0 {
		return nil
	}
	out := make([]metav1.Condition, len(in))
	copy(out, in)
	return out
}

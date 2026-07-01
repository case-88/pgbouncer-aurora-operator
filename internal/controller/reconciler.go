package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"github.com/case-88/pgbouncer-aurora-operator/internal/planner"
	"github.com/case-88/pgbouncer-aurora-operator/internal/render"
)

type Discovery interface {
	Discover(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (domain.DiscoveryResult, error)
}

type Monitor interface {
	Check(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) (map[string]domain.HealthStatus, error)
}

type PgBouncerAuroraReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Discovery Discovery
	Monitor   Monitor

	MaxConcurrentReconciles int
	WatchName               string
	ReconcileMinInterval    time.Duration
	ScheduleEvents          <-chan event.GenericEvent

	lastReconcileMu      sync.Mutex
	lastReconcileStarted map[types.NamespacedName]time.Time
}

const restartedAtAnnotation = "pgbouncer-aurora.io/restarted-at"

const (
	defaultDiscoveryInterval = 3 * time.Second
	defaultMonitorInterval   = 10 * time.Second
	minCheckInterval         = time.Second
	defaultMaxReconciles     = 2
)

func (r *PgBouncerAuroraReconciler) refreshResourceFromAPI(ctx context.Context, key types.NamespacedName, resource *v1alpha1.PgBouncerAurora) error {
	if r.APIReader == nil {
		return nil
	}
	fresh := &v1alpha1.PgBouncerAurora{}
	if err := r.getFreshResource(ctx, key, fresh); err != nil {
		return err
	}
	*resource = *fresh.DeepCopy()
	return nil
}

func (r *PgBouncerAuroraReconciler) getFreshResource(ctx context.Context, key types.NamespacedName, resource *v1alpha1.PgBouncerAurora) error {
	if r.APIReader != nil {
		return r.APIReader.Get(ctx, key, resource)
	}
	return r.Get(ctx, key, resource)
}

func (r *PgBouncerAuroraReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.matchesWatchName(req.Name) {
		return ctrl.Result{}, nil
	}
	started := time.Now()
	log := ctrl.Log.WithName("reconciler").WithValues("namespace", req.Namespace, "cr", req.Name)
	ctx = ctrl.LoggerInto(ctx, log)
	if remaining := r.reconcileThrottleRemaining(req.NamespacedName, started); remaining > 0 {
		log.V(1).Info("reconcile throttled", "remaining", remaining.String(), "reconcileMinInterval", r.reconcileMinInterval().String())
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	resource := &v1alpha1.PgBouncerAurora{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			r.forgetReconcileThrottle(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if err := r.refreshResourceFromAPI(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			r.forgetReconcileThrottle(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	log.V(1).Info("reconcile started", "generation", resource.Generation, "observedGeneration", resource.Status.ObservedGeneration)
	if r.Discovery == nil {
		return ctrl.Result{}, fmt.Errorf("discovery is required")
	}

	now := metav1.Now()
	discoveryRan := false
	discoveryFailed := false
	discovery := cachedDiscovery(resource)
	var err error
	if r.ScheduleEvents == nil {
		discovery, discoveryRan, discoveryFailed, err = r.discoveryFor(ctx, resource, now)
		if err != nil {
			log.Error(err, "discovery errored")
			return ctrl.Result{}, err
		}
	}
	log.V(1).Info("discovery evaluated",
		"ran", discoveryRan,
		"trusted", discovery.Trusted,
		"failed", discoveryFailed,
		"reason", truncateLogValue(discovery.Reason),
		"instances", len(discovery.Instances),
		"writer", countDiscoveredRole(discovery.Instances, domain.RoleWriter),
		"reader", countDiscoveredRole(discovery.Instances, domain.RoleReader),
	)
	if discoveryFailed || (!discovery.Trusted && !conditionStatusIs(resource.Status.Conditions, "DiscoveryTrusted", metav1.ConditionFalse)) {
		log.Error(logReasonError(discovery.Reason), "discovery failed",
			"ran", discoveryRan,
			"trusted", discovery.Trusted,
			"failed", discoveryFailed,
			"cached", discoveryFailed && discovery.Trusted,
			"failureCount", nextDiscoveryFailureCount(resource, discoveryFailed),
			"threshold", discoveryFailureThreshold(resource),
			"reason", truncateLogValue(discovery.Reason),
			"instances", len(discovery.Instances),
		)
	}
	if discoveryRan && discovery.Trusted && topologyChanged(discovery.Instances, resource.Status.LastKnownTopology.Instances) {
		diff := topologyDiffForLog(discovery.Instances, resource.Status.LastKnownTopology.Instances)
		log.Info("topology changed",
			"added", diff.Added,
			"removed", diff.Removed,
			"changed", diff.Changed,
			"instances", len(discovery.Instances),
			"writer", countDiscoveredRole(discovery.Instances, domain.RoleWriter),
			"reader", countDiscoveredRole(discovery.Instances, domain.RoleReader),
			"topologyHashBefore", resource.Status.TopologyHash,
			"topologyHashAfter", hashObject(topologyStatus(discovery.Instances)),
		)
	}
	health := healthFromStatus(resource.Status.Instances)
	monitorRan := false
	cachedHealth := true
	monitorErr := ""
	if r.ScheduleEvents == nil {
		health, monitorRan, cachedHealth, monitorErr, err = r.healthFor(ctx, resource, discovery, discoveryRan, now)
		if err != nil {
			log.Error(err, "monitor errored")
			return ctrl.Result{}, err
		}
	}
	unhealthy := countUnhealthy(health)
	log.V(1).Info("monitor evaluated",
		"ran", monitorRan,
		"cached", cachedHealth,
		"error", truncateLogValue(monitorErr),
		"healthy", countHealthy(health),
		"unhealthy", unhealthy,
		"instances", len(health),
	)
	if monitorErr != "" {
		log.Error(logReasonError(monitorErr), "monitor failed",
			"ran", monitorRan,
			"cached", cachedHealth,
			"healthy", countHealthy(health),
			"unhealthy", unhealthy,
			"instances", len(health),
			"error", truncateLogValue(monitorErr),
		)
	} else if unhealthy > 0 {
		log.Error(fmt.Errorf("%d unhealthy backend(s)", unhealthy), "monitor reported unhealthy backends",
			"ran", monitorRan,
			"cached", cachedHealth,
			"healthy", countHealthy(health),
			"unhealthy", unhealthy,
			"instances", len(health),
		)
	}
	missingInstances := cloneMissingInstances(resource.Status.MissingInstances)
	if discoveryRan {
		missingInstances = missingInstancesFor(resource, discovery, discoveryRan, now)
	}

	plan := planner.Plan(planner.Input{
		Resource:         resource,
		DiscoveryTrusted: discovery.Trusted,
		Discovered:       discovery.Instances,
		Health:           health,
		CachedHealth:     cachedHealth,
		MissingInstances: missingInstances,
	})
	if err := r.applyWriterFailoverFastPath(ctx, resource, discovery, &plan); err != nil {
		return ctrl.Result{}, err
	}
	applyZoneAwareConflictPolicy(resource, &plan)
	if monitorRan && monitorErr == "" {
		if diff := healthDiffForLog(resource.Status.Instances, plan.Instances); len(diff.Changed) > 0 {
			log.Info("backend health changed",
				"changed", diff.Changed,
				"healthy", countHealthy(health),
				"unhealthy", unhealthy,
				"instances", len(health),
			)
		}
	}

	shouldApply, applyReason, err := r.shouldApplyPlan(ctx, resource, plan, missingInstances, discoveryRan, monitorRan, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	eventReport := topologyEventReport(resource, plan)
	planLog := log.V(1)
	if shouldApply {
		planLog = log
	}
	planLog.Info("plan evaluated",
		"apply", shouldApply,
		"applyReason", applyReason,
		"frozen", plan.Frozen,
		"planInstances", len(plan.Instances),
		"writerMembers", len(plan.Membership.Writer),
		"readerMembers", len(plan.Membership.Reader),
		"missingInstances", len(missingInstances),
	)
	if plan.Frozen && !conditionStatusIs(resource.Status.Conditions, "Frozen", metav1.ConditionTrue) {
		log.Error(logReasonError(planReasonsMessage(plan)), "plan frozen",
			"apply", shouldApply,
			"applyReason", applyReason,
			"reasons", planReasonsMessage(plan),
			"planInstances", len(plan.Instances),
			"writerMembers", len(plan.Membership.Writer),
			"readerMembers", len(plan.Membership.Reader),
		)
	}
	if shouldApply {
		if eventReport.Event != "" {
			log.Info("topology event detected",
				"event", eventReport.Event,
				"applyReason", applyReason,
				"oldWriter", eventReport.OldWriter,
				"newWriter", eventReport.NewWriter,
				"addedReaders", eventReport.AddedReaders,
				"removedReaders", eventReport.RemovedReaders,
				"writerMembersBefore", resource.Status.LastAppliedMembership.Writer,
				"writerMembersAfter", plan.Membership.Writer,
				"readerMembersBefore", resource.Status.LastAppliedMembership.Reader,
				"readerMembersAfter", plan.Membership.Reader,
				"connectionHandling", effectiveWriterChangeConnectionHandling(resource),
			)
		}
		applyStarted := time.Now()
		if err := r.applyDesired(ctx, resource, plan); err != nil {
			log.Error(err, "apply desired failed")
			return ctrl.Result{}, err
		}
		var err error
		missingInstances, err = r.cleanupRemovedInstanceResources(ctx, resource, discovery, discoveryRan, missingInstances, now)
		if err != nil {
			log.Error(err, "cleanup removed instance resources failed")
			return ctrl.Result{}, err
		}
		if err := r.patchPodMembership(ctx, resource, plan); err != nil {
			log.Error(err, "patch pod membership failed")
			return ctrl.Result{}, err
		}
		if err := r.handleWriterChangeConnection(ctx, resource, plan); err != nil {
			log.Error(err, "writer change connection handling failed")
			return ctrl.Result{}, err
		}
		log.Info("apply completed",
			"duration", time.Since(applyStarted).String(),
			"applyReason", applyReason,
			"writerMembersBefore", resource.Status.LastAppliedMembership.Writer,
			"writerMembersAfter", plan.Membership.Writer,
			"readerMembersBefore", resource.Status.LastAppliedMembership.Reader,
			"readerMembersAfter", plan.Membership.Reader,
		)
		if eventReport.Event != "" {
			log.Info("topology event handled",
				"event", eventReport.Event,
				"result", "applied",
				"applyReason", applyReason,
				"oldWriter", eventReport.OldWriter,
				"newWriter", eventReport.NewWriter,
				"addedReaders", eventReport.AddedReaders,
				"removedReaders", eventReport.RemovedReaders,
				"writerMembersBefore", resource.Status.LastAppliedMembership.Writer,
				"writerMembersAfter", plan.Membership.Writer,
				"readerMembersBefore", resource.Status.LastAppliedMembership.Reader,
				"readerMembersAfter", plan.Membership.Reader,
				"connectionHandling", effectiveWriterChangeConnectionHandling(resource),
				"duration", time.Since(applyStarted).String(),
			)
		}
	}
	if err := r.updateStatus(ctx, resource, discovery, plan, missingInstances, discoveryRan, discoveryFailed, monitorRan, monitorErr, now); err != nil {
		log.Error(err, "status update failed")
		return ctrl.Result{}, err
	}
	result := ctrl.Result{}
	if r.ScheduleEvents == nil {
		result = ctrl.Result{RequeueAfter: requeueAfter(resource)}
	}
	log.V(1).Info("reconcile completed", "requeueAfter", result.RequeueAfter.String(), "duration", time.Since(started).String())
	return result, nil
}

func (r *PgBouncerAuroraReconciler) applyWriterFailoverFastPath(ctx context.Context, resource *v1alpha1.PgBouncerAurora, discovery domain.DiscoveryResult, plan *planner.Output) error {
	if resource == nil || plan == nil || plan.Frozen || !discovery.Trusted {
		return nil
	}
	newWriter := singleDiscoveredWriter(discovery.Instances)
	if newWriter == "" || sameStringSet(resource.Status.LastAppliedMembership.Writer, []string{newWriter}) || len(resource.Status.LastAppliedMembership.Writer) == 0 {
		return nil
	}
	if instanceDisabled(resource.Spec.PgBouncer, newWriter) {
		return nil
	}
	if sameStringSet(plan.Membership.Writer, []string{newWriter}) {
		return nil
	}
	readyCounts, err := r.readyPodCounts(ctx, resource)
	if err != nil {
		return err
	}
	if readyCounts[newWriter] == 0 {
		return nil
	}
	plan.Membership.Writer = []string{newWriter}
	for i := range plan.Instances {
		if plan.Instances[i].Name == newWriter && plan.Instances[i].ReadyReplicas < readyCounts[newWriter] {
			plan.Instances[i].ReadyReplicas = readyCounts[newWriter]
		}
	}
	plan.Reasons = append(plan.Reasons, "writer failover fast path")
	return nil
}

func singleDiscoveredWriter(instances []domain.InstanceObservation) string {
	writer := ""
	for _, instance := range instances {
		if instance.Role != domain.RoleWriter {
			continue
		}
		if writer != "" {
			return ""
		}
		writer = instance.Name
	}
	return writer
}

func sameStringSet(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := stringSet(left)
	for _, value := range right {
		if !seen[value] {
			return false
		}
	}
	return true
}

func (r *PgBouncerAuroraReconciler) shouldApplyPlan(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output, missingInstances []v1alpha1.MissingInstanceStatus, discoveryRan bool, monitorRan bool, now metav1.Time) (bool, string, error) {
	if plan.Frozen {
		return false, "frozen", nil
	}
	if resource.Generation != resource.Status.ObservedGeneration ||
		resource.Status.LastAppliedTime == nil {
		return true, "generation-or-never-applied", nil
	}
	if discoveryRan || monitorRan {
		if planDesiredStateChanged(resource, plan) {
			return true, "desired-state-changed", nil
		}
	}
	if removedInstanceDeletionDue(resource, missingInstances, now) {
		return true, "removed-instance-retention-expired", nil
	}
	needsApply, reason, err := r.cachedPlanNeedsApply(ctx, resource, plan)
	if err != nil {
		return false, "", err
	}
	return needsApply, reason, nil
}

func countDiscoveredRole(instances []domain.InstanceObservation, role domain.Role) int {
	count := 0
	for _, instance := range instances {
		if instance.Role == role {
			count++
		}
	}
	return count
}

func countHealthy(health map[string]domain.HealthStatus) int {
	count := 0
	for _, status := range health {
		if status.Healthy {
			count++
		}
	}
	return count
}

func countUnhealthy(health map[string]domain.HealthStatus) int {
	count := 0
	for _, status := range health {
		if !status.Healthy {
			count++
		}
	}
	return count
}

func truncateLogValue(value string) string {
	const max = 300
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

func planDesiredStateChanged(resource *v1alpha1.PgBouncerAurora, plan planner.Output) bool {
	return membershipChanged(resource.Status.LastAppliedMembership.Writer, plan.Membership.Writer) ||
		membershipChanged(resource.Status.LastAppliedMembership.Reader, plan.Membership.Reader) ||
		hashObject(instanceApplyShapeFromStatus(resource.Status.Instances)) != hashObject(instanceApplyShapeFromPlan(plan.Instances))
}

type topologyEventLog struct {
	Event          string
	OldWriter      string
	NewWriter      string
	AddedReaders   []string
	RemovedReaders []string
}

func topologyEventReport(resource *v1alpha1.PgBouncerAurora, plan planner.Output) topologyEventLog {
	previousWriter := singleString(resource.Status.LastAppliedMembership.Writer)
	nextWriter := singleString(plan.Membership.Writer)
	addedReaders := stringSetDiff(plan.Membership.Reader, resource.Status.LastAppliedMembership.Reader)
	removedReaders := stringSetDiff(resource.Status.LastAppliedMembership.Reader, plan.Membership.Reader)
	report := topologyEventLog{
		OldWriter:      previousWriter,
		NewWriter:      nextWriter,
		AddedReaders:   addedReaders,
		RemovedReaders: removedReaders,
	}
	if previousWriter != "" && nextWriter != "" && previousWriter != nextWriter {
		report.Event = "failover"
		return report
	}
	if len(addedReaders) > 0 && len(removedReaders) > 0 {
		report.Event = "reader_membership_changed"
		return report
	}
	if len(addedReaders) > 0 {
		report.Event = "reader_added"
		return report
	}
	if len(removedReaders) > 0 {
		report.Event = "reader_removed"
		return report
	}
	return report
}

func singleString(values []string) string {
	if len(values) == 1 {
		return values[0]
	}
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ",")
}

func stringSetDiff(left []string, right []string) []string {
	rightSet := stringSet(right)
	out := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; !ok {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

type instanceApplyShape struct {
	Name             string        `json:"name,omitempty"`
	Endpoint         string        `json:"endpoint,omitempty"`
	Port             int32         `json:"port,omitempty"`
	Role             v1alpha1.Role `json:"role,omitempty"`
	AvailabilityZone string        `json:"availabilityZone,omitempty"`
	DbiResourceId    string        `json:"dbiResourceId,omitempty"`
	Disabled         bool          `json:"disabled,omitempty"`
	Replicas         int32         `json:"replicas,omitempty"`
}

func instanceApplyShapeFromPlan(instances []domain.InstancePlan) []instanceApplyShape {
	out := make([]instanceApplyShape, 0, len(instances))
	for _, instance := range instances {
		out = append(out, instanceApplyShape{
			Name:             instance.Name,
			Endpoint:         instance.Endpoint,
			Port:             instance.Port,
			Role:             instance.Role,
			AvailabilityZone: instance.AvailabilityZone,
			DbiResourceId:    instance.DbiResourceId,
			Disabled:         instance.Disabled,
			Replicas:         instance.Replicas,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func instanceApplyShapeFromStatus(instances []v1alpha1.InstanceStatus) []instanceApplyShape {
	out := make([]instanceApplyShape, 0, len(instances))
	for _, instance := range instances {
		out = append(out, instanceApplyShape{
			Name:             instance.InstanceName,
			Endpoint:         instance.Endpoint,
			Port:             instance.Port,
			Role:             instance.Role,
			AvailabilityZone: instance.AvailabilityZone,
			DbiResourceId:    instance.DbiResourceId,
			Disabled:         instance.Disabled,
			Replicas:         instance.DesiredReplicas,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func removedInstanceDeletionDue(resource *v1alpha1.PgBouncerAurora, missingInstances []v1alpha1.MissingInstanceStatus, now metav1.Time) bool {
	for _, missing := range missingInstances {
		if removedInstanceReadyForDelete(resource, missing, now) {
			return true
		}
	}
	return false
}

func (r *PgBouncerAuroraReconciler) cachedPlanNeedsApply(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) (bool, string, error) {
	expectedNames := expectedManagedResourceNames(resource, plan)
	for _, list := range []struct {
		items client.ObjectList
		kind  string
	}{
		{items: &corev1.ConfigMapList{}, kind: "configmap"},
		{items: &appsv1.DeploymentList{}, kind: "deployment"},
		{items: &corev1.ServiceList{}, kind: "service"},
	} {
		if err := r.List(ctx, list.items, client.InNamespace(resource.Namespace), client.MatchingLabels{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   render.ClusterLabelValue(resource.Name),
		}); err != nil {
			return false, "", err
		}
		missing, missingName, err := missingExpectedNames(list.items, expectedNames[list.kind])
		if err != nil {
			return false, "", err
		}
		if missing {
			return true, list.kind + "-missing:" + missingName, nil
		}
		if list.kind == "configmap" && configMapDrifted(resource, plan, list.items.(*corev1.ConfigMapList)) {
			return true, "configmap-drifted", nil
		}
		if list.kind == "deployment" {
			drifted, err := r.deploymentTemplateDrifted(ctx, resource, plan, list.items.(*appsv1.DeploymentList))
			if err != nil {
				return false, "", err
			}
			if drifted {
				return true, "deployment-template-drifted", nil
			}
		}
		if list.kind == "service" {
			services := list.items.(*corev1.ServiceList)
			if staleRoleServiceExists(resource, services, expectedNames[list.kind]) {
				return true, "stale-role-service-exists", nil
			}
			if serviceDrifted(resource, plan, services) {
				return true, "service-drifted", nil
			}
		}
	}
	drifted, err := r.podMembershipLabelDrifted(ctx, resource, plan)
	if err != nil {
		return false, "", err
	}
	if drifted {
		return true, "pod-membership-label-drifted", nil
	}
	return false, "no-change", nil
}

func configMapDrifted(resource *v1alpha1.PgBouncerAurora, plan planner.Output, configMaps *corev1.ConfigMapList) bool {
	byName := make(map[string]*corev1.ConfigMap, len(configMaps.Items))
	for i := range configMaps.Items {
		configMap := &configMaps.Items[i]
		byName[configMap.Name] = configMap
	}
	for _, instance := range activePlanInstances(plan.Instances) {
		expected := render.ConfigMap(resource, instance.InstanceObservation)
		existing := byName[expected.Name]
		if existing == nil {
			continue
		}
		if !reflect.DeepEqual(existing.Data, expected.Data) || !reflect.DeepEqual(existing.BinaryData, expected.BinaryData) {
			return true
		}
	}
	return false
}

func serviceDrifted(resource *v1alpha1.PgBouncerAurora, plan planner.Output, services *corev1.ServiceList) bool {
	byName := make(map[string]*corev1.Service, len(services.Items))
	for i := range services.Items {
		service := &services.Items[i]
		byName[service.Name] = service
	}
	for _, instance := range activePlanInstances(plan.Instances) {
		expected := render.InstanceService(resource, instance)
		existing := byName[expected.Name]
		if existing != nil && serviceSpecDrifted(existing, expected) {
			return true
		}
	}
	for _, role := range []v1alpha1.Role{v1alpha1.RoleWriter, v1alpha1.RoleReader} {
		expected := render.RoleService(resource, role)
		if expected == nil {
			continue
		}
		existing := byName[expected.Name]
		if existing != nil && serviceSpecDrifted(existing, expected) {
			return true
		}
	}
	return false
}

func serviceSpecDrifted(existing *corev1.Service, expected *corev1.Service) bool {
	return existing.Spec.Type != expected.Spec.Type ||
		!reflect.DeepEqual(existing.Spec.Selector, expected.Spec.Selector) ||
		!servicePortsEqualIgnoringAllocated(existing.Spec.Ports, expected.Spec.Ports) ||
		!reflect.DeepEqual(existing.Annotations, expected.Annotations)
}

func servicePortsEqualIgnoringAllocated(left []corev1.ServicePort, right []corev1.ServicePort) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name ||
			left[i].Protocol != right[i].Protocol ||
			left[i].Port != right[i].Port ||
			left[i].TargetPort != right[i].TargetPort ||
			left[i].AppProtocol == nil && right[i].AppProtocol != nil ||
			left[i].AppProtocol != nil && right[i].AppProtocol == nil {
			return false
		}
		if left[i].AppProtocol != nil && right[i].AppProtocol != nil && *left[i].AppProtocol != *right[i].AppProtocol {
			return false
		}
	}
	return true
}

func (r *PgBouncerAuroraReconciler) deploymentTemplateDrifted(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output, deployments *appsv1.DeploymentList) (bool, error) {
	if len(activePlanInstances(plan.Instances)) == 0 {
		return false, nil
	}
	byName := make(map[string]*appsv1.Deployment, len(deployments.Items))
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		byName[deployment.Name] = deployment
	}
	authFileHash := r.authFileHash(ctx, resource)
	for _, instance := range activePlanInstances(plan.Instances) {
		expected := render.InstanceDeployment(render.InstanceRenderInput{Owner: resource, Instance: instance, AuthFileHash: authFileHash})
		existing := byName[expected.Name]
		if existing == nil {
			continue
		}
		if int32Value(existing.Spec.Replicas) != int32Value(expected.Spec.Replicas) ||
			existing.Spec.Strategy.Type != expected.Spec.Strategy.Type ||
			!podTemplateSemanticallyEqual(existing.Spec.Template, expected.Spec.Template) {
			return true, nil
		}
	}
	return false, nil
}

func podTemplateSemanticallyEqual(existing corev1.PodTemplateSpec, expected corev1.PodTemplateSpec) bool {
	normalizePodTemplateForDrift(&existing)
	normalizePodTemplateForDrift(&expected)
	return apiequality.Semantic.DeepEqual(existing, expected)
}

func normalizePodTemplateForDrift(template *corev1.PodTemplateSpec) {
	if template.Annotations != nil {
		delete(template.Annotations, restartedAtAnnotation)
		if len(template.Annotations) == 0 {
			template.Annotations = nil
		}
	}
	if len(template.Labels) == 0 {
		template.Labels = nil
	}
	if template.Spec.DNSPolicy == corev1.DNSClusterFirst {
		template.Spec.DNSPolicy = ""
	}
	if template.Spec.SchedulerName == corev1.DefaultSchedulerName {
		template.Spec.SchedulerName = ""
	}
	if template.Spec.TerminationGracePeriodSeconds != nil && *template.Spec.TerminationGracePeriodSeconds == 30 {
		template.Spec.TerminationGracePeriodSeconds = nil
	}
	if template.Spec.SecurityContext != nil && reflect.DeepEqual(template.Spec.SecurityContext, &corev1.PodSecurityContext{}) {
		template.Spec.SecurityContext = nil
	}
	if template.Spec.EnableServiceLinks != nil && *template.Spec.EnableServiceLinks {
		template.Spec.EnableServiceLinks = nil
	}
	for i := range template.Spec.Volumes {
		normalizeVolumeForDrift(&template.Spec.Volumes[i])
	}
	for i := range template.Spec.Containers {
		normalizeContainerForDrift(&template.Spec.Containers[i])
	}
	for i := range template.Spec.InitContainers {
		normalizeContainerForDrift(&template.Spec.InitContainers[i])
	}
}

func normalizeContainerForDrift(container *corev1.Container) {
	if container.TerminationMessagePath == "/dev/termination-log" {
		container.TerminationMessagePath = ""
	}
	if container.TerminationMessagePolicy == corev1.TerminationMessageReadFile {
		container.TerminationMessagePolicy = ""
	}
	if container.SecurityContext != nil && reflect.DeepEqual(container.SecurityContext, &corev1.SecurityContext{}) {
		container.SecurityContext = nil
	}
	normalizeProbeForDrift(container.ReadinessProbe)
	normalizeProbeForDrift(container.LivenessProbe)
	normalizeProbeForDrift(container.StartupProbe)
	for i := range container.Ports {
		if container.Ports[i].Protocol == corev1.ProtocolTCP {
			container.Ports[i].Protocol = ""
		}
	}
}

func normalizeProbeForDrift(probe *corev1.Probe) {
	if probe == nil {
		return
	}
	if probe.SuccessThreshold == 1 {
		probe.SuccessThreshold = 0
	}
	if probe.FailureThreshold == 3 {
		probe.FailureThreshold = 0
	}
}

func normalizeVolumeForDrift(volume *corev1.Volume) {
	if volume.ConfigMap != nil && volume.ConfigMap.DefaultMode != nil && *volume.ConfigMap.DefaultMode == 420 {
		volume.ConfigMap.DefaultMode = nil
	}
	if volume.Secret != nil && volume.Secret.DefaultMode != nil && *volume.Secret.DefaultMode == 420 {
		volume.Secret.DefaultMode = nil
	}
}

func int32Value(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func templateAnnotation(deployment *appsv1.Deployment, key string) string {
	if deployment == nil || deployment.Spec.Template.Annotations == nil {
		return ""
	}
	return deployment.Spec.Template.Annotations[key]
}

func expectedManagedResourceNames(resource *v1alpha1.PgBouncerAurora, plan planner.Output) map[string]map[string]bool {
	out := map[string]map[string]bool{
		"configmap":  {},
		"deployment": {},
		"service":    {},
	}
	for _, instance := range activePlanInstances(plan.Instances) {
		name := render.InstanceResourceName(resource.Name, instance.Name)
		out["configmap"][name] = true
		out["deployment"][name] = true
		out["service"][name] = true
	}
	for _, role := range []v1alpha1.Role{v1alpha1.RoleWriter, v1alpha1.RoleReader} {
		if service := render.RoleService(resource, role); service != nil {
			out["service"][service.Name] = true
		}
	}
	return out
}

func missingExpectedNames(list client.ObjectList, expected map[string]bool) (bool, string, error) {
	if len(expected) == 0 {
		return false, "", nil
	}
	seen := map[string]bool{}
	items, err := listItems(list)
	if err != nil {
		return false, "", err
	}
	for _, item := range items {
		seen[item.GetName()] = true
	}
	for name := range expected {
		if !seen[name] {
			return true, name, nil
		}
	}
	return false, "", nil
}

func staleRoleServiceExists(resource *v1alpha1.PgBouncerAurora, services *corev1.ServiceList, desiredNames map[string]bool) bool {
	for i := range services.Items {
		service := &services.Items[i]
		if service.Labels[render.LabelInstance] != "" {
			continue
		}
		if service.Labels[render.LabelServiceRole] == "" && !isLegacyRoleService(resource, service) {
			continue
		}
		if desiredNames[service.Name] {
			continue
		}
		return true
	}
	return false
}

func listItems(list client.ObjectList) ([]client.Object, error) {
	switch typed := list.(type) {
	case *corev1.ConfigMapList:
		out := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			out = append(out, &typed.Items[i])
		}
		return out, nil
	case *appsv1.DeploymentList:
		out := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			out = append(out, &typed.Items[i])
		}
		return out, nil
	case *corev1.ServiceList:
		out := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			out = append(out, &typed.Items[i])
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported list type %T", list)
	}
}

func (r *PgBouncerAuroraReconciler) podMembershipLabelDrifted(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(resource.Namespace), client.MatchingLabels{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
	}); err != nil {
		return false, err
	}
	writer := stringSet(plan.Membership.Writer)
	reader := stringSet(plan.Membership.Reader)
	roles := instanceRoleSet(activePlanInstances(plan.Instances))
	for i := range pods.Items {
		pod := &pods.Items[i]
		instanceName := pod.Labels[render.LabelInstance]
		if instanceName == "" {
			continue
		}
		if pod.Labels[render.LabelWriter] != boolLabel(writer[instanceName]) ||
			pod.Labels[render.LabelReader] != boolLabel(reader[instanceName]) ||
			pod.Labels[render.LabelRole] != string(roles[instanceName]) {
			return true, nil
		}
	}
	return false, nil
}

func (r *PgBouncerAuroraReconciler) SetupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PgBouncerAurora{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.requestsForManagedPod))
	if r.ScheduleEvents != nil {
		builder = builder.WatchesRawSource(source.Channel(r.ScheduleEvents, &handler.EnqueueRequestForObject{}))
	}
	return builder.
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: r.maxConcurrentReconciles()}).
		Complete(r)
}

func (r *PgBouncerAuroraReconciler) maxConcurrentReconciles() int {
	if r.MaxConcurrentReconciles > 0 {
		return r.MaxConcurrentReconciles
	}
	return defaultMaxReconciles
}

func (r *PgBouncerAuroraReconciler) reconcileMinInterval() time.Duration {
	if r.ReconcileMinInterval > 0 {
		return r.ReconcileMinInterval
	}
	return 0
}

func (r *PgBouncerAuroraReconciler) forgetReconcileThrottle(key types.NamespacedName) {
	r.lastReconcileMu.Lock()
	defer r.lastReconcileMu.Unlock()
	delete(r.lastReconcileStarted, key)
}

func (r *PgBouncerAuroraReconciler) reconcileThrottleRemaining(key types.NamespacedName, now time.Time) time.Duration {
	minInterval := r.reconcileMinInterval()
	if minInterval <= 0 {
		return 0
	}
	r.lastReconcileMu.Lock()
	defer r.lastReconcileMu.Unlock()
	if r.lastReconcileStarted == nil {
		r.lastReconcileStarted = map[types.NamespacedName]time.Time{}
	}
	last, ok := r.lastReconcileStarted[key]
	if ok {
		elapsed := now.Sub(last)
		if elapsed < minInterval {
			return minInterval - elapsed
		}
	}
	r.lastReconcileStarted[key] = now
	return 0
}

func (r *PgBouncerAuroraReconciler) requestsForManagedPod(ctx context.Context, object client.Object) []ctrl.Request {
	_ = ctx
	labels := object.GetLabels()
	if labels[render.LabelManagedBy] != render.ManagedByValue || labels[render.LabelCluster] == "" {
		return nil
	}
	clusterName := object.GetAnnotations()[render.AnnotationClusterName]
	if clusterName == "" {
		clusterName = labels[render.LabelCluster]
	}
	if !r.matchesWatchName(clusterName) {
		return nil
	}
	return []ctrl.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: object.GetNamespace(),
			Name:      clusterName,
		},
	}}
}

func (r *PgBouncerAuroraReconciler) matchesWatchName(name string) bool {
	return matchesWatchNames(r.WatchName, name)
}

func requeueAfter(resource *v1alpha1.PgBouncerAurora) time.Duration {
	discoveryInterval := discoveryInterval(resource)
	monitorInterval := monitorInterval(resource)
	if monitorInterval < discoveryInterval {
		return monitorInterval
	}
	return discoveryInterval
}

func discoveryInterval(resource *v1alpha1.PgBouncerAurora) time.Duration {
	return boundedCheckInterval(resource.Spec.Discovery.Interval.Duration, defaultDiscoveryInterval)
}

func monitorInterval(resource *v1alpha1.PgBouncerAurora) time.Duration {
	return boundedCheckInterval(resource.Spec.Monitor.Interval.Duration, defaultMonitorInterval)
}

func boundedCheckInterval(value time.Duration, fallback time.Duration) time.Duration {
	if value <= 0 {
		value = fallback
	}
	if value < minCheckInterval {
		return minCheckInterval
	}
	return value
}

func (r *PgBouncerAuroraReconciler) discoveryFor(ctx context.Context, resource *v1alpha1.PgBouncerAurora, now metav1.Time) (domain.DiscoveryResult, bool, bool, error) {
	if discoveryDue(resource, now) {
		discovery, err := r.Discovery.Discover(ctx, resource)
		if err != nil || discovery.Trusted {
			return discovery, true, false, err
		}
		return discoveryAfterFailureThreshold(resource, discovery), true, true, nil
	}
	if !lastDiscoveryTrusted(resource) {
		return domain.DiscoveryResult{Trusted: false, Reason: "discovery retry interval not due"}, false, false, nil
	}
	return domain.DiscoveryResult{
		Trusted:   true,
		Instances: topologyObservations(resource.Status.LastKnownTopology.Instances),
		Reason:    lastDiscoveryMessage(resource),
	}, false, false, nil
}

func discoveryAfterFailureThreshold(resource *v1alpha1.PgBouncerAurora, failed domain.DiscoveryResult) domain.DiscoveryResult {
	failures := resource.Status.ConsecutiveDiscoveryFailures + 1
	threshold := discoveryFailureThreshold(resource)
	if failures >= threshold || !lastDiscoveryTrusted(resource) || len(resource.Status.LastKnownTopology.Instances) == 0 {
		return failed
	}
	reason := "using cached discovery after transient failure"
	if failed.Reason != "" {
		reason = fmt.Sprintf("%s %d/%d: %s", reason, failures, threshold, failed.Reason)
	}
	return domain.DiscoveryResult{
		Trusted:   true,
		Instances: topologyObservations(resource.Status.LastKnownTopology.Instances),
		Reason:    reason,
	}
}

func discoveryFailureThreshold(resource *v1alpha1.PgBouncerAurora) int32 {
	if resource.Spec.Discovery.FailureThreshold > 0 {
		return resource.Spec.Discovery.FailureThreshold
	}
	return 3
}

func (r *PgBouncerAuroraReconciler) healthFor(
	ctx context.Context,
	resource *v1alpha1.PgBouncerAurora,
	discovery domain.DiscoveryResult,
	discoveryRan bool,
	now metav1.Time,
) (map[string]domain.HealthStatus, bool, bool, string, error) {
	if !discovery.Trusted {
		return map[string]domain.HealthStatus{}, false, false, "", nil
	}
	if r.Monitor == nil {
		return healthFromStatus(resource.Status.Instances), false, true, "", nil
	}
	shouldRunMonitor := monitorDue(resource, now) ||
		topologyChanged(discovery.Instances, resource.Status.LastKnownTopology.Instances) ||
		(discoveryRan && len(resource.Status.Instances) == 0)
	if !shouldRunMonitor {
		readinessChanged, err := r.podReadinessChanged(ctx, resource)
		if err != nil {
			return nil, false, false, "", err
		}
		shouldRunMonitor = readinessChanged
	}
	if shouldRunMonitor {
		health, err := r.Monitor.Check(ctx, resource, activeDiscoveryInstances(resource, discovery.Instances))
		if err != nil {
			return healthFromStatus(resource.Status.Instances), false, true, err.Error(), nil
		}
		return health, true, false, "", nil
	}
	return healthFromStatus(resource.Status.Instances), false, true, "", nil
}

func discoveryDue(resource *v1alpha1.PgBouncerAurora, now metav1.Time) bool {
	return resource.Generation != resource.Status.ObservedGeneration ||
		resource.Status.LastDiscoveryTime == nil ||
		now.Time.Sub(resource.Status.LastDiscoveryTime.Time) >= discoveryInterval(resource)
}

func monitorDue(resource *v1alpha1.PgBouncerAurora, now metav1.Time) bool {
	return resource.Generation != resource.Status.ObservedGeneration ||
		resource.Status.LastMonitorTime == nil ||
		len(resource.Status.Instances) == 0 ||
		now.Time.Sub(resource.Status.LastMonitorTime.Time) >= monitorInterval(resource)
}

func (r *PgBouncerAuroraReconciler) podReadinessChanged(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (bool, error) {
	readyCounts, err := r.readyPodCounts(ctx, resource)
	if err != nil {
		return false, err
	}
	for _, status := range resource.Status.Instances {
		if status.InstanceName == "" {
			continue
		}
		if readyCounts[status.InstanceName] != status.ReadyReplicas {
			return true, nil
		}
		delete(readyCounts, status.InstanceName)
	}
	return len(readyCounts) > 0, nil
}

func (r *PgBouncerAuroraReconciler) readyPodCounts(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (map[string]int32, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(resource.Namespace), client.MatchingLabels{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
	}); err != nil {
		return nil, err
	}
	readyCounts := map[string]int32{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podReady(pod) {
			continue
		}
		instanceName := pod.Labels[render.LabelInstance]
		if instanceName != "" {
			readyCounts[instanceName]++
		}
	}
	return readyCounts, nil
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func lastDiscoveryTrusted(resource *v1alpha1.PgBouncerAurora) bool {
	for _, condition := range resource.Status.Conditions {
		if condition.Type == "DiscoveryTrusted" {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}

func lastDiscoveryMessage(resource *v1alpha1.PgBouncerAurora) string {
	for _, condition := range resource.Status.Conditions {
		if condition.Type == "DiscoveryTrusted" && condition.Message != "" {
			return condition.Message
		}
	}
	return "using cached discovery"
}

func topologyObservations(statuses []v1alpha1.InstanceTopologyStatus) []domain.InstanceObservation {
	out := make([]domain.InstanceObservation, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, domain.InstanceObservation{
			Name:             status.InstanceName,
			Endpoint:         status.Endpoint,
			Port:             status.Port,
			Role:             status.Role,
			AvailabilityZone: status.AvailabilityZone,
			DbiResourceId:    status.DbiResourceId,
		})
	}
	return out
}

func activeDiscoveryInstances(resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) []domain.InstanceObservation {
	out := make([]domain.InstanceObservation, 0, len(instances))
	for _, instance := range instances {
		if resource != nil && instanceDisabled(resource.Spec.PgBouncer, instance.Name) {
			continue
		}
		out = append(out, instance)
	}
	return out
}

func healthFromStatus(statuses []v1alpha1.InstanceStatus) map[string]domain.HealthStatus {
	out := make(map[string]domain.HealthStatus, len(statuses))
	for _, status := range statuses {
		if status.InstanceName == "" {
			continue
		}
		out[status.InstanceName] = domain.HealthStatus{Healthy: status.Healthy, Reason: status.Reason}
	}
	return out
}

func topologyChanged(instances []domain.InstanceObservation, statuses []v1alpha1.InstanceTopologyStatus) bool {
	if len(instances) != len(statuses) {
		return true
	}
	statusByName := make(map[string]v1alpha1.InstanceTopologyStatus, len(statuses))
	for _, status := range statuses {
		statusByName[status.InstanceName] = status
	}
	for _, instance := range instances {
		status, ok := statusByName[instance.Name]
		if !ok ||
			status.Endpoint != instance.Endpoint ||
			status.Port != instance.Port ||
			status.Role != instance.Role ||
			status.AvailabilityZone != instance.AvailabilityZone ||
			status.DbiResourceId != instance.DbiResourceId {
			return true
		}
	}
	return false
}

func (r *PgBouncerAuroraReconciler) applyDesired(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) error {
	authFileHash := ""
	activeInstances := activePlanInstances(plan.Instances)
	if len(activeInstances) > 0 {
		authFileHash = r.authFileHash(ctx, resource)
	}
	for _, instance := range activeInstances {
		cm := render.ConfigMap(resource, instance.InstanceObservation)
		deployment := render.InstanceDeployment(render.InstanceRenderInput{Owner: resource, Instance: instance, AuthFileHash: authFileHash})
		service := render.InstanceService(resource, instance)
		for _, object := range []client.Object{cm, deployment, service} {
			if err := r.setOwner(resource, object); err != nil {
				return err
			}
			if err := r.applyObject(ctx, object); err != nil {
				return err
			}
		}
	}
	for _, role := range []v1alpha1.Role{v1alpha1.RoleWriter, v1alpha1.RoleReader} {
		service := render.RoleService(resource, role)
		if service == nil {
			continue
		}
		if err := r.setOwner(resource, service); err != nil {
			return err
		}
		if err := r.applyObject(ctx, service); err != nil {
			return err
		}
	}
	if err := r.cleanupStaleRoleServices(ctx, resource); err != nil {
		return err
	}
	if err := r.cleanupDisabledInstanceResources(ctx, resource, plan); err != nil {
		return err
	}
	return nil
}

func (r *PgBouncerAuroraReconciler) cleanupDisabledInstanceResources(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) error {
	for _, instance := range plan.Instances {
		if !instance.Disabled {
			continue
		}
		if err := r.deleteInstanceResources(ctx, resource, instance.Name); err != nil {
			return err
		}
	}
	return nil
}

func (r *PgBouncerAuroraReconciler) cleanupStaleRoleServices(ctx context.Context, resource *v1alpha1.PgBouncerAurora) error {
	desiredNames := map[string]bool{}
	for _, role := range []v1alpha1.Role{v1alpha1.RoleWriter, v1alpha1.RoleReader} {
		if service := render.RoleService(resource, role); service != nil {
			desiredNames[service.Name] = true
		}
	}
	services := &corev1.ServiceList{}
	if err := r.List(ctx, services, client.InNamespace(resource.Namespace), client.MatchingLabels{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
	}); err != nil {
		return err
	}
	for i := range services.Items {
		service := &services.Items[i]
		if service.Labels[render.LabelInstance] != "" {
			continue
		}
		if service.Labels[render.LabelServiceRole] == "" && !isLegacyRoleService(resource, service) {
			continue
		}
		if desiredNames[service.Name] {
			continue
		}
		if err := r.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func isLegacyRoleService(resource *v1alpha1.PgBouncerAurora, service *corev1.Service) bool {
	if service == nil || service.Labels[render.LabelInstance] != "" {
		return false
	}
	return service.Spec.Selector[render.LabelWriter] == "true" ||
		service.Spec.Selector[render.LabelReader] == "true" ||
		service.Name == serviceNameForRole(resource, v1alpha1.RoleWriter) ||
		service.Name == serviceNameForRole(resource, v1alpha1.RoleReader)
}

func (r *PgBouncerAuroraReconciler) authFileHash(ctx context.Context, resource *v1alpha1.PgBouncerAurora) string {
	secretName := resource.Spec.PgBouncer.AuthFileSecretRef.Name
	if secretName == "" {
		return ""
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: resource.Namespace}, secret); err != nil {
		return ""
	}
	return secretDataHash(secret.Data)
}

func secretDataHash(data map[string][]byte) string {
	if len(data) == 0 {
		return ""
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write(data[key])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (r *PgBouncerAuroraReconciler) cleanupRemovedInstanceResources(
	ctx context.Context,
	resource *v1alpha1.PgBouncerAurora,
	discovery domain.DiscoveryResult,
	discoveryRan bool,
	missingInstances []v1alpha1.MissingInstanceStatus,
	now metav1.Time,
) ([]v1alpha1.MissingInstanceStatus, error) {
	_ = discoveryRan
	if !discovery.Trusted {
		return missingInstances, nil
	}
	current := discoveredInstanceSet(discovery.Instances)
	remaining := make([]v1alpha1.MissingInstanceStatus, 0, len(missingInstances))
	for _, missing := range missingInstances {
		if current[missing.InstanceName] || !removedInstanceReadyForDelete(resource, missing, now) {
			remaining = append(remaining, missing)
			continue
		}
		if err := r.deleteInstanceResources(ctx, resource, missing.InstanceName); err != nil {
			return nil, err
		}
	}
	return remaining, nil
}

func removedInstanceReadyForDelete(resource *v1alpha1.PgBouncerAurora, missing v1alpha1.MissingInstanceStatus, now metav1.Time) bool {
	if missing.InstanceName == "" || missing.MissingCount < removeAfterMissingCount(resource) || missing.FirstMissingTime == nil {
		return false
	}
	return now.Time.Sub(missing.FirstMissingTime.Time) >= removedInstanceRetention(resource)
}

func removeAfterMissingCount(resource *v1alpha1.PgBouncerAurora) int32 {
	if resource.Spec.TopologyPolicy.RemoveAfterMissingCount > 0 {
		return resource.Spec.TopologyPolicy.RemoveAfterMissingCount
	}
	return 3
}

func removedInstanceRetention(resource *v1alpha1.PgBouncerAurora) time.Duration {
	if resource.Spec.TopologyPolicy.RemovedInstanceRetention.Duration > 0 {
		return resource.Spec.TopologyPolicy.RemovedInstanceRetention.Duration
	}
	return time.Hour
}

func (r *PgBouncerAuroraReconciler) deleteInstanceResources(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instanceName string) error {
	name := render.InstanceResourceName(resource.Name, instanceName)
	objects := []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
	}
	for _, object := range objects {
		if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *PgBouncerAuroraReconciler) handleWriterChangeConnection(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) error {
	policy := effectiveWriterChangeConnectionHandling(resource)
	if policy == v1alpha1.WriterChangeKeepExisting || !membershipChanged(resource.Status.LastAppliedMembership.Writer, plan.Membership.Writer) {
		return nil
	}
	if len(resource.Status.LastAppliedMembership.Writer) == 0 {
		return nil
	}

	names := map[string]bool{}
	switch policy {
	case v1alpha1.WriterChangeRestartWriters:
		for _, name := range resource.Status.LastAppliedMembership.Writer {
			names[name] = true
		}
		for _, name := range plan.Membership.Writer {
			names[name] = true
		}
	case v1alpha1.WriterChangeRestartAll:
		for _, instance := range activePlanInstances(plan.Instances) {
			names[instance.Name] = true
		}
		for _, name := range resource.Status.LastAppliedMembership.Writer {
			names[name] = true
		}
	default:
		return nil
	}
	token := writerRestartToken(resource, plan, policy, names)
	return r.restartInstanceDeployments(ctx, resource, names, token)
}

func effectiveWriterChangeConnectionHandling(resource *v1alpha1.PgBouncerAurora) v1alpha1.WriterChangeConnectionHandling {
	if resource == nil || resource.Spec.TopologyPolicy.WriterChangeConnectionHandling == "" {
		return v1alpha1.WriterChangeRestartWriters
	}
	return resource.Spec.TopologyPolicy.WriterChangeConnectionHandling
}

func (r *PgBouncerAuroraReconciler) restartInstanceDeployments(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instanceNames map[string]bool, token string) error {
	for _, instanceName := range sortedBoolMapKeys(instanceNames) {
		key := types.NamespacedName{Name: render.InstanceResourceName(resource.Name, instanceName), Namespace: resource.Namespace}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			deployment := &appsv1.Deployment{}
			if err := r.Get(ctx, key, deployment); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}
			if deployment.Spec.Template.Annotations == nil {
				deployment.Spec.Template.Annotations = map[string]string{}
			}
			if deployment.Spec.Template.Annotations[restartedAtAnnotation] == token {
				return nil
			}
			deployment.Spec.Template.Annotations[restartedAtAnnotation] = token
			return r.Update(ctx, deployment)
		}); err != nil {
			return err
		}
	}
	return nil
}

type writerRestartTokenPayload struct {
	Policy         v1alpha1.WriterChangeConnectionHandling `json:"policy,omitempty"`
	PreviousWriter []string                                `json:"previousWriter,omitempty"`
	DesiredWriter  []string                                `json:"desiredWriter,omitempty"`
	RestartTargets []string                                `json:"restartTargets,omitempty"`
	Instances      []instanceApplyShape                    `json:"instances,omitempty"`
}

func writerRestartToken(resource *v1alpha1.PgBouncerAurora, plan planner.Output, policy v1alpha1.WriterChangeConnectionHandling, instanceNames map[string]bool) string {
	previousWriter := sortedStrings(resource.Status.LastAppliedMembership.Writer)
	desiredWriter := sortedStrings(plan.Membership.Writer)
	return hashObject(writerRestartTokenPayload{
		Policy:         policy,
		PreviousWriter: previousWriter,
		DesiredWriter:  desiredWriter,
		RestartTargets: sortedBoolMapKeys(instanceNames),
		Instances:      instanceApplyShapeFromPlan(plan.Instances),
	})
}

func membershipChanged(a, b []string) bool {
	if len(a) != len(b) {
		return true
	}
	left := stringSet(a)
	for _, value := range b {
		if !left[value] {
			return true
		}
	}
	return false
}

func (r *PgBouncerAuroraReconciler) setOwner(owner *v1alpha1.PgBouncerAurora, object client.Object) error {
	if r.Scheme == nil {
		return nil
	}
	return controllerutil.SetControllerReference(owner, object, r.Scheme)
}

func (r *PgBouncerAuroraReconciler) applyObject(ctx context.Context, desired client.Object) error {
	key := types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := emptyObjectFor(desired)
		if err != nil {
			return err
		}
		if err := r.Get(ctx, key, existing); err != nil {
			if apierrors.IsNotFound(err) {
				if err := r.Create(ctx, desired); err != nil {
					if apierrors.IsAlreadyExists(err) {
						return apierrors.NewConflict(schema.GroupResource{Resource: "objects"}, desired.GetName(), err)
					}
					return err
				}
				return nil
			}
			return err
		}
		before := existing.DeepCopyObject().(client.Object)
		copyDesired(existing, desired)
		if reflect.DeepEqual(before, existing) {
			return nil
		}
		return r.Update(ctx, existing)
	})
}

func emptyObjectFor(object client.Object) (client.Object, error) {
	switch object.(type) {
	case *corev1.ConfigMap:
		return &corev1.ConfigMap{}, nil
	case *corev1.Service:
		return &corev1.Service{}, nil
	case *appsv1.Deployment:
		return &appsv1.Deployment{}, nil
	default:
		return nil, fmt.Errorf("unsupported object type %T", object)
	}
}

func copyDesired(existing client.Object, desired client.Object) {
	existing.SetLabels(desired.GetLabels())
	existing.SetAnnotations(desired.GetAnnotations())
	existing.SetOwnerReferences(desired.GetOwnerReferences())
	switch target := existing.(type) {
	case *corev1.ConfigMap:
		source := desired.(*corev1.ConfigMap)
		target.Data = source.Data
		target.BinaryData = source.BinaryData
	case *corev1.Service:
		source := desired.(*corev1.Service)
		allocated := target.Spec.DeepCopy()
		target.Spec = source.Spec
		preserveServiceAllocatedFields(&target.Spec, allocated)
	case *appsv1.Deployment:
		source := desired.(*appsv1.Deployment)
		restartToken := ""
		if target.Spec.Template.Annotations != nil {
			restartToken = target.Spec.Template.Annotations[restartedAtAnnotation]
		}
		target.Spec = source.Spec
		preserveRestartAnnotation(target, restartToken)
	}
}

func preserveServiceAllocatedFields(target *corev1.ServiceSpec, allocated *corev1.ServiceSpec) {
	if allocated == nil {
		return
	}
	if allocated.ClusterIP != "" {
		target.ClusterIP = allocated.ClusterIP
		target.ClusterIPs = cloneStrings(allocated.ClusterIPs)
	}
	if len(allocated.IPFamilies) > 0 {
		target.IPFamilies = append([]corev1.IPFamily(nil), allocated.IPFamilies...)
	}
	if allocated.IPFamilyPolicy != nil {
		policy := *allocated.IPFamilyPolicy
		target.IPFamilyPolicy = &policy
	}
	if allocated.LoadBalancerClass != nil {
		loadBalancerClass := *allocated.LoadBalancerClass
		target.LoadBalancerClass = &loadBalancerClass
	}
	if shouldPreserveHealthCheckNodePort(target) && allocated.HealthCheckNodePort != 0 {
		target.HealthCheckNodePort = allocated.HealthCheckNodePort
	}
	if shouldPreserveNodePorts(target) {
		preserveServiceNodePorts(target.Ports, allocated.Ports)
	}
}

func shouldPreserveNodePorts(target *corev1.ServiceSpec) bool {
	return target.Type == corev1.ServiceTypeNodePort || target.Type == corev1.ServiceTypeLoadBalancer
}

func shouldPreserveHealthCheckNodePort(target *corev1.ServiceSpec) bool {
	return target.Type == corev1.ServiceTypeLoadBalancer &&
		target.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyLocal
}

func preserveServiceNodePorts(target []corev1.ServicePort, allocated []corev1.ServicePort) {
	nodePorts := map[string]int32{}
	for _, port := range allocated {
		if port.NodePort == 0 {
			continue
		}
		nodePorts[servicePortKey(port)] = port.NodePort
	}
	for i := range target {
		if nodePort := nodePorts[servicePortKey(target[i])]; nodePort != 0 {
			target[i].NodePort = nodePort
		}
	}
}

func servicePortKey(port corev1.ServicePort) string {
	if port.Name != "" {
		return "name:" + port.Name
	}
	return fmt.Sprintf("%s:%d", port.Protocol, port.Port)
}

func preserveRestartAnnotation(target *appsv1.Deployment, restartToken string) {
	if restartToken == "" {
		return
	}
	if target.Spec.Template.Annotations == nil {
		target.Spec.Template.Annotations = map[string]string{}
	}
	target.Spec.Template.Annotations[restartedAtAnnotation] = restartToken
}

func (r *PgBouncerAuroraReconciler) patchPodMembership(ctx context.Context, resource *v1alpha1.PgBouncerAurora, plan planner.Output) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(resource.Namespace), client.MatchingLabels{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
	}); err != nil {
		return err
	}
	writer := stringSet(plan.Membership.Writer)
	reader := stringSet(plan.Membership.Reader)
	roles := instanceRoleSet(activePlanInstances(plan.Instances))
	for i := range pods.Items {
		updated, err := r.patchPodMembershipAdditions(ctx, &pods.Items[i], writer, reader, roles)
		if err != nil {
			return err
		}
		if updated != nil {
			pods.Items[i] = *updated
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		instanceName := pod.Labels[render.LabelInstance]
		desiredWriter := writer[instanceName]
		desiredReader := reader[instanceName]
		desiredRole := roles[instanceName]
		if pod.Labels[render.LabelWriter] == boolLabel(desiredWriter) &&
			pod.Labels[render.LabelReader] == boolLabel(desiredReader) &&
			pod.Labels[render.LabelRole] == string(desiredRole) {
			continue
		}
		patched := pod.DeepCopy()
		if patched.Labels == nil {
			patched.Labels = map[string]string{}
		}
		setBoolLabel(patched.Labels, render.LabelWriter, desiredWriter)
		setBoolLabel(patched.Labels, render.LabelReader, desiredReader)
		if desiredRole != "" {
			patched.Labels[render.LabelRole] = string(desiredRole)
		} else {
			delete(patched.Labels, render.LabelRole)
		}
		if err := r.Patch(ctx, patched, client.MergeFrom(pod)); err != nil {
			return err
		}
	}
	return nil
}

func (r *PgBouncerAuroraReconciler) patchPodMembershipAdditions(
	ctx context.Context,
	pod *corev1.Pod,
	writer map[string]bool,
	reader map[string]bool,
	roles map[string]v1alpha1.Role,
) (*corev1.Pod, error) {
	instanceName := pod.Labels[render.LabelInstance]
	desiredWriter := writer[instanceName]
	desiredReader := reader[instanceName]
	desiredRole := roles[instanceName]
	if (!desiredWriter || pod.Labels[render.LabelWriter] == "true") &&
		(!desiredReader || pod.Labels[render.LabelReader] == "true") &&
		(desiredRole == "" || pod.Labels[render.LabelRole] == string(desiredRole)) {
		return nil, nil
	}
	patched := pod.DeepCopy()
	if patched.Labels == nil {
		patched.Labels = map[string]string{}
	}
	if desiredWriter {
		patched.Labels[render.LabelWriter] = "true"
	}
	if desiredReader {
		patched.Labels[render.LabelReader] = "true"
	}
	if desiredRole != "" {
		patched.Labels[render.LabelRole] = string(desiredRole)
	}
	if err := r.Patch(ctx, patched, client.MergeFrom(pod)); err != nil {
		return nil, err
	}
	return patched, nil
}

func instanceRoleSet(instances []domain.InstancePlan) map[string]v1alpha1.Role {
	out := make(map[string]v1alpha1.Role, len(instances))
	for _, instance := range instances {
		if instance.Name != "" {
			out[instance.Name] = instance.Role
		}
	}
	return out
}

func activePlanInstances(instances []domain.InstancePlan) []domain.InstancePlan {
	out := make([]domain.InstancePlan, 0, len(instances))
	for _, instance := range instances {
		if instance.Disabled {
			continue
		}
		out = append(out, instance)
	}
	return out
}

func instanceDisabled(spec v1alpha1.PgBouncerSpec, instanceName string) bool {
	for _, override := range spec.InstanceOverrides {
		if override.Name == instanceName && override.Enabled != nil && !*override.Enabled {
			return true
		}
	}
	return false
}

func stringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func sortedStrings(values []string) []string {
	out := cloneStrings(values)
	sort.Strings(out)
	return out
}

func sortedBoolMapKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return ""
}

func setBoolLabel(labels map[string]string, key string, value bool) {
	if value {
		labels[key] = "true"
		return
	}
	delete(labels, key)
}

func (r *PgBouncerAuroraReconciler) updateStatus(
	ctx context.Context,
	resource *v1alpha1.PgBouncerAurora,
	discovery domain.DiscoveryResult,
	plan planner.Output,
	missingInstances []v1alpha1.MissingInstanceStatus,
	discoveryRan bool,
	discoveryFailed bool,
	monitorRan bool,
	monitorErr string,
	now metav1.Time,
) error {
	key := types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1alpha1.PgBouncerAurora{}
		if err := r.getFreshResource(ctx, key, fresh); err != nil {
			return err
		}
		beforeStatus := hashObject(fresh.Status)
		previousConditions := fresh.Status.Conditions
		fresh.Status.ObservedGeneration = resource.Generation
		discoveryStatusIsNewer := fresh.Status.LastDiscoveryTime != nil && fresh.Status.LastDiscoveryTime.Time.After(now.Time)
		monitorStatusIsNewer := fresh.Status.LastMonitorTime != nil && fresh.Status.LastMonitorTime.Time.After(now.Time)
		if discoveryRan && !discoveryStatusIsNewer {
			fresh.Status.LastDiscoveryTime = &now
			if discoveryFailed {
				fresh.Status.ConsecutiveDiscoveryFailures = resource.Status.ConsecutiveDiscoveryFailures + 1
			} else {
				fresh.Status.ConsecutiveDiscoveryFailures = 0
			}
		}
		if monitorRan && !monitorStatusIsNewer {
			fresh.Status.LastMonitorTime = &now
		}
		if discovery.Trusted && discoveryRan && !discoveryStatusIsNewer {
			fresh.Status.LastKnownTopology.Instances = topologyStatus(discovery.Instances)
			fresh.Status.MissingInstances = missingInstances
			fresh.Status.TopologyHash = hashObject(fresh.Status.LastKnownTopology.Instances)
		}
		if discovery.Trusted && !discoveryRan && hashObject(fresh.Status.MissingInstances) != hashObject(missingInstances) {
			fresh.Status.MissingInstances = missingInstances
		}
		if !plan.Frozen && ((monitorRan && !monitorStatusIsNewer) || len(fresh.Status.Instances) == 0) {
			fresh.Status.Instances = instanceStatus(plan.Instances)
		}
		if !monitorStatusIsNewer {
			fresh.Status.ServiceSummary = serviceSummaryStatus(resource, plan)
		}
		if !plan.Frozen {
			membersChanged := membershipChanged(fresh.Status.LastAppliedMembership.Writer, plan.Membership.Writer) ||
				membershipChanged(fresh.Status.LastAppliedMembership.Reader, plan.Membership.Reader)
			if membersChanged || fresh.Status.LastAppliedTime == nil {
				fresh.Status.LastAppliedTime = &now
			}
			if membersChanged {
				fresh.Status.LastAppliedMembership.Writer = cloneStrings(plan.Membership.Writer)
				fresh.Status.LastAppliedMembership.Reader = cloneStrings(plan.Membership.Reader)
			}
		}
		fresh.Status.MembershipHash = hashObject(fresh.Status.LastAppliedMembership)
		fresh.Status.Conditions = conditionsFor(resource, previousConditions, discovery, plan, monitorErr, now)
		if beforeStatus == hashObject(fresh.Status) {
			return nil
		}
		return r.Status().Update(ctx, fresh)
	})
}

func hashObject(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func missingInstancesFor(resource *v1alpha1.PgBouncerAurora, discovery domain.DiscoveryResult, discoveryRan bool, now metav1.Time) []v1alpha1.MissingInstanceStatus {
	if !discovery.Trusted || !discoveryRan {
		return cloneMissingInstances(resource.Status.MissingInstances)
	}
	current := discoveredInstanceSet(discovery.Instances)
	existing := map[string]v1alpha1.MissingInstanceStatus{}
	candidates := map[string]bool{}
	fastRemove := stringSet(discovery.RemovingInstances)
	for _, missing := range resource.Status.MissingInstances {
		if missing.InstanceName == "" || current[missing.InstanceName] {
			continue
		}
		existing[missing.InstanceName] = missing
		candidates[missing.InstanceName] = true
	}
	for _, previous := range resource.Status.LastKnownTopology.Instances {
		if previous.InstanceName == "" || current[previous.InstanceName] {
			continue
		}
		candidates[previous.InstanceName] = true
	}

	names := make([]string, 0, len(candidates))
	for name := range candidates {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]v1alpha1.MissingInstanceStatus, 0, len(names))
	for _, name := range names {
		item := existing[name]
		item.InstanceName = name
		item.MissingCount++
		if fastRemove[name] && item.MissingCount < removeAfterMissingCount(resource) {
			item.MissingCount = removeAfterMissingCount(resource)
		}
		if item.FirstMissingTime == nil {
			item.FirstMissingTime = cloneTime(now)
		}
		item.LastMissingTime = cloneTime(now)
		out = append(out, item)
	}
	return out
}

func discoveredInstanceSet(instances []domain.InstanceObservation) map[string]bool {
	out := make(map[string]bool, len(instances))
	for _, instance := range instances {
		if instance.Name != "" {
			out[instance.Name] = true
		}
	}
	return out
}

func topologyStatus(instances []domain.InstanceObservation) []v1alpha1.InstanceTopologyStatus {
	out := make([]v1alpha1.InstanceTopologyStatus, 0, len(instances))
	for _, instance := range instances {
		out = append(out, v1alpha1.InstanceTopologyStatus{
			InstanceName:     instance.Name,
			Endpoint:         instance.Endpoint,
			Port:             instance.Port,
			Role:             instance.Role,
			AvailabilityZone: instance.AvailabilityZone,
			DbiResourceId:    instance.DbiResourceId,
		})
	}
	return out
}

func instanceStatus(instances []domain.InstancePlan) []v1alpha1.InstanceStatus {
	out := make([]v1alpha1.InstanceStatus, 0, len(instances))
	for _, instance := range instances {
		out = append(out, v1alpha1.InstanceStatus{
			InstanceName:         instance.Name,
			Endpoint:             instance.Endpoint,
			Port:                 instance.Port,
			Role:                 instance.Role,
			AvailabilityZone:     instance.AvailabilityZone,
			DbiResourceId:        instance.DbiResourceId,
			Disabled:             instance.Disabled,
			Healthy:              instance.Healthy,
			DesiredReplicas:      instance.Replicas,
			ReadyReplicas:        instance.ReadyReplicas,
			ConsecutiveFailures:  instance.ConsecutiveFailures,
			ConsecutiveSuccesses: instance.ConsecutiveSuccesses,
			Reason:               instance.Reason,
		})
	}
	return out
}

func serviceSummaryStatus(resource *v1alpha1.PgBouncerAurora, plan planner.Output) v1alpha1.ServiceSummaryStatus {
	activeInstances := activePlanInstances(plan.Instances)
	return v1alpha1.ServiceSummaryStatus{
		Writer: roleServiceSummary(resource, activeInstances, plan.Membership.Writer, v1alpha1.RoleWriter),
		Reader: roleServiceSummary(resource, activeInstances, plan.Membership.Reader, v1alpha1.RoleReader),
	}
}

func roleServiceSummary(resource *v1alpha1.PgBouncerAurora, instances []domain.InstancePlan, members []string, role v1alpha1.Role) v1alpha1.RoleServiceSummaryStatus {
	summary := v1alpha1.RoleServiceSummaryStatus{
		ServiceName: serviceNameForRole(resource, role),
		DesiredRole: role,
		Members:     int32(len(members)),
	}
	byName := make(map[string]domain.InstancePlan, len(instances))
	countedCandidates := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		byName[instance.Name] = instance
		if instance.Role != role {
			continue
		}
		countedCandidates[instance.Name] = struct{}{}
		summary.TotalCandidates++
		if instance.Healthy {
			summary.Healthy++
		} else {
			summary.Unhealthy++
		}
	}
	for _, member := range members {
		instance, ok := byName[member]
		if !ok {
			continue
		}
		if instance.ReadyReplicas > 0 {
			summary.ReadyMembers++
		}
		if role == v1alpha1.RoleReader && instance.Role == v1alpha1.RoleWriter {
			summary.FallbackFromWriter = true
			if _, counted := countedCandidates[member]; counted {
				continue
			}
			countedCandidates[member] = struct{}{}
			summary.TotalCandidates++
			if instance.Healthy {
				summary.Healthy++
			} else {
				summary.Unhealthy++
			}
		}
	}
	return summary
}

func serviceNameForRole(resource *v1alpha1.PgBouncerAurora, role v1alpha1.Role) string {
	spec := resource.Spec.Services.Writer
	defaultName := "writer"
	if role == v1alpha1.RoleReader {
		spec = resource.Spec.Services.Reader.ServiceRoleSpec
		defaultName = "reader"
	}
	name := spec.Name
	if name == "" {
		name = defaultName
	}
	return render.RoleServiceName(resource.Name, name)
}

func conditionsFor(resource *v1alpha1.PgBouncerAurora, previous []metav1.Condition, discovery domain.DiscoveryResult, plan planner.Output, monitorErr string, now metav1.Time) []metav1.Condition {
	discoveryStatus := metav1.ConditionFalse
	discoveryReason := "DiscoveryUntrusted"
	if discovery.Trusted {
		discoveryStatus = metav1.ConditionTrue
		discoveryReason = "DiscoveryTrusted"
	}
	monitorStatus := metav1.ConditionTrue
	monitorReason := "MonitorSucceeded"
	if monitorErr != "" {
		monitorStatus = metav1.ConditionFalse
		monitorReason = "MonitorFailed"
	}
	reconcileStatus := metav1.ConditionTrue
	reconcileReason := "Applied"
	if plan.Frozen {
		reconcileStatus = metav1.ConditionFalse
		reconcileReason = "Frozen"
	}
	frozenStatus := metav1.ConditionFalse
	frozenReason := "NotFrozen"
	if plan.Frozen {
		frozenStatus = metav1.ConditionTrue
		frozenReason = "Frozen"
	}
	backendHealthyStatus, backendHealthyReason, backendHealthyMessage := backendHealthyCondition(plan)
	writerReadyStatus, writerReadyReason, writerReadyMessage := roleReadyCondition(resource, plan, v1alpha1.RoleWriter)
	readerReadyStatus, readerReadyReason, readerReadyMessage := roleReadyCondition(resource, plan, v1alpha1.RoleReader)
	fallbackStatus := metav1.ConditionFalse
	fallbackReason := "ReaderNormal"
	if readerFallbackFromWriter(plan) {
		fallbackStatus = metav1.ConditionTrue
		fallbackReason = "FallbackFromWriter"
	}
	roleMismatchStatus := metav1.ConditionFalse
	roleMismatchReason := "NoRoleMismatch"
	roleMismatchMessage := roleMismatchMessage(plan)
	if roleMismatchMessage != "" {
		roleMismatchStatus = metav1.ConditionTrue
		roleMismatchReason = "RoleMismatch"
	}
	trafficTransitionStatus, trafficTransitionReason, trafficTransitionMessage := trafficTransitionCondition(discovery, plan)
	zoneAwareConflictStatus, zoneAwareConflictReason, zoneAwareConflictMessage := zoneAwareConflictCondition(resource, plan)
	degradedStatus, degradedReason, degradedMessage := degradedCondition(
		discovery,
		plan,
		monitorErr,
		backendHealthyStatus,
		backendHealthyMessage,
		writerReadyStatus,
		writerReadyMessage,
		readerReadyStatus,
		readerReadyMessage,
		roleMismatchMessage,
	)
	return []metav1.Condition{
		conditionWithTransition(previous, "DiscoveryTrusted", discoveryStatus, discoveryReason, discovery.Reason, now),
		conditionWithTransition(previous, "MonitorSucceeded", monitorStatus, monitorReason, monitorErr, now),
		conditionWithTransition(previous, "Reconciled", reconcileStatus, reconcileReason, "", now),
		conditionWithTransition(previous, "Frozen", frozenStatus, frozenReason, strings.Join(plan.Reasons, "; "), now),
		conditionWithTransition(previous, "BackendHealthy", backendHealthyStatus, backendHealthyReason, backendHealthyMessage, now),
		conditionWithTransition(previous, "WriterReady", writerReadyStatus, writerReadyReason, writerReadyMessage, now),
		conditionWithTransition(previous, "ReaderReady", readerReadyStatus, readerReadyReason, readerReadyMessage, now),
		conditionWithTransition(previous, "ReaderFallback", fallbackStatus, fallbackReason, "", now),
		conditionWithTransition(previous, "RoleMismatch", roleMismatchStatus, roleMismatchReason, roleMismatchMessage, now),
		conditionWithTransition(previous, "TrafficTransitioning", trafficTransitionStatus, trafficTransitionReason, trafficTransitionMessage, now),
		conditionWithTransition(previous, "ZoneAwareConflict", zoneAwareConflictStatus, zoneAwareConflictReason, zoneAwareConflictMessage, now),
		conditionWithTransition(previous, "Degraded", degradedStatus, degradedReason, degradedMessage, now),
	}
}

func applyZoneAwareConflictPolicy(resource *v1alpha1.PgBouncerAurora, plan *planner.Output) {
	if plan == nil || plan.Frozen || resource == nil {
		return
	}
	policy := resource.Spec.TopologyPolicy.ZoneAware.ConflictPolicy
	if policy != v1alpha1.ZoneAwareConflictFail {
		return
	}
	status, reason, message := zoneAwareConflictCondition(resource, *plan)
	if status != metav1.ConditionTrue {
		return
	}
	plan.Frozen = true
	if message == "" {
		message = reason
	}
	plan.Reasons = append(plan.Reasons, "zoneAware conflict: "+message)
}

func zoneAwareConflictCondition(resource *v1alpha1.PgBouncerAurora, plan planner.Output) (metav1.ConditionStatus, string, string) {
	if resource == nil || !enabledDefaultTrue(resource.Spec.TopologyPolicy.ZoneAware.Enabled) {
		return metav1.ConditionFalse, "ZoneAwareDisabled", ""
	}
	zoneAware := resource.Spec.TopologyPolicy.ZoneAware
	policy := zoneAware.ConflictPolicy
	if policy == "" {
		policy = v1alpha1.ZoneAwareConflictWarn
	}
	if policy == v1alpha1.ZoneAwareConflictIgnore {
		return metav1.ConditionFalse, "ConflictIgnored", ""
	}
	topologyKey := firstNonEmpty(zoneAware.TopologyKey, "topology.kubernetes.io/zone")
	messages := make([]string, 0)
	nodeSelector := effectiveNodeSelector(resource)
	for _, instance := range activePlanInstances(plan.Instances) {
		if instance.AvailabilityZone == "" {
			continue
		}
		if selected, ok := nodeSelector[topologyKey]; ok && selected != instance.AvailabilityZone {
			messages = append(messages, fmt.Sprintf("%s requires %s=%s but nodeSelector fixes %s=%s", instance.Name, topologyKey, instance.AvailabilityZone, topologyKey, selected))
		}
		if requiredNodeAffinityConflicts(resource.Spec.PgBouncer.Affinity, topologyKey, instance.AvailabilityZone) {
			messages = append(messages, fmt.Sprintf("%s requires %s=%s but required nodeAffinity excludes it", instance.Name, topologyKey, instance.AvailabilityZone))
		}
	}
	if zoneAware.Enforcement == v1alpha1.ZoneAwareRequired && hasTopologySpreadOnKey(resource, topologyKey) {
		messages = append(messages, fmt.Sprintf("zoneAware Required pins pods to a single %s while topologySpreadConstraints also spread by %s", topologyKey, topologyKey))
	}
	if len(messages) == 0 {
		return metav1.ConditionFalse, "NoZoneAwareConflict", ""
	}
	sort.Strings(messages)
	reason := "ZoneAwareConflictWarn"
	if policy == v1alpha1.ZoneAwareConflictFail {
		reason = "ZoneAwareConflictFail"
	}
	return metav1.ConditionTrue, reason, strings.Join(messages, "; ")
}

func effectiveNodeSelector(resource *v1alpha1.PgBouncerAurora) map[string]string {
	out := map[string]string{}
	if resource == nil {
		return out
	}
	for key, value := range resource.Spec.PgBouncer.NodeSelector {
		out[key] = value
	}
	return out
}

func requiredNodeAffinityConflicts(affinity *corev1.Affinity, topologyKey string, zone string) bool {
	if affinity == nil || affinity.NodeAffinity == nil || affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		if nodeSelectorTermAllowsZone(term, topologyKey, zone) {
			return false
		}
	}
	return true
}

func nodeSelectorTermAllowsZone(term corev1.NodeSelectorTerm, topologyKey string, zone string) bool {
	for _, expression := range term.MatchExpressions {
		if expression.Key != topologyKey {
			continue
		}
		switch expression.Operator {
		case corev1.NodeSelectorOpIn:
			return stringSliceContains(expression.Values, zone)
		case corev1.NodeSelectorOpNotIn:
			return !stringSliceContains(expression.Values, zone)
		case corev1.NodeSelectorOpExists:
			return true
		case corev1.NodeSelectorOpDoesNotExist:
			return false
		}
	}
	for _, field := range term.MatchFields {
		if field.Key == topologyKey {
			return true
		}
	}
	return true
}

func hasTopologySpreadOnKey(resource *v1alpha1.PgBouncerAurora, topologyKey string) bool {
	if resource == nil {
		return false
	}
	for _, constraint := range resource.Spec.PgBouncer.TopologySpreadConstraints {
		if constraint.TopologyKey == topologyKey {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func trafficTransitionCondition(discovery domain.DiscoveryResult, plan planner.Output) (metav1.ConditionStatus, string, string) {
	messages := make([]string, 0)
	members := make(map[string]struct{}, len(plan.Membership.Writer)+len(plan.Membership.Reader))
	for _, name := range plan.Membership.Writer {
		members[name] = struct{}{}
	}
	for _, name := range plan.Membership.Reader {
		members[name] = struct{}{}
	}
	pending := make([]string, 0)
	for _, instance := range activePlanInstances(plan.Instances) {
		if _, ok := members[instance.Name]; ok {
			continue
		}
		if instance.Healthy && instance.ReadyReplicas > 0 {
			continue
		}
		reason := instance.Reason
		if reason == "" {
			reason = "waiting for health/readiness"
		}
		pending = append(pending, fmt.Sprintf("%s:%s", instance.Name, reason))
	}
	if len(pending) > 0 {
		sort.Strings(pending)
		messages = append(messages, fmt.Sprintf("pendingMembership=%s", strings.Join(pending, ",")))
	}
	if len(messages) == 0 {
		return metav1.ConditionFalse, "TrafficStable", ""
	}
	return metav1.ConditionTrue, "TrafficTransitioning", strings.Join(messages, "; ")
}

func backendHealthyCondition(plan planner.Output) (metav1.ConditionStatus, string, string) {
	instances := activePlanInstances(plan.Instances)
	if len(instances) == 0 {
		return metav1.ConditionFalse, "NoBackends", "no backend instances"
	}
	unhealthy := make([]string, 0)
	for _, instance := range instances {
		if instance.Healthy {
			continue
		}
		message := instance.Name
		if instance.Reason != "" {
			message = fmt.Sprintf("%s: %s", instance.Name, instance.Reason)
		}
		unhealthy = append(unhealthy, message)
	}
	if len(unhealthy) > 0 {
		return metav1.ConditionFalse, "BackendUnhealthy", strings.Join(unhealthy, "; ")
	}
	return metav1.ConditionTrue, "AllBackendsHealthy", ""
}

func roleReadyCondition(resource *v1alpha1.PgBouncerAurora, plan planner.Output, role v1alpha1.Role) (metav1.ConditionStatus, string, string) {
	members := plan.Membership.Writer
	roleName := "Writer"
	if role == v1alpha1.RoleReader {
		members = plan.Membership.Reader
		roleName = "Reader"
	}
	if len(members) == 0 {
		return metav1.ConditionFalse, fmt.Sprintf("No%sMembers", roleName), ""
	}
	activeInstances := activePlanInstances(plan.Instances)
	instances := make(map[string]domain.InstancePlan, len(activeInstances))
	for _, instance := range activeInstances {
		instances[instance.Name] = instance
	}
	notReady := make([]string, 0)
	for _, member := range members {
		instance, ok := instances[member]
		if ok && instance.ReadyReplicas > 0 {
			return metav1.ConditionTrue, fmt.Sprintf("%sReady", roleName), ""
		}
		if ok {
			notReady = append(notReady, fmt.Sprintf("%s readyReplicas=%d", member, instance.ReadyReplicas))
			continue
		}
		notReady = append(notReady, fmt.Sprintf("%s missing from plan", member))
	}
	return metav1.ConditionFalse, fmt.Sprintf("%sPodsNotReady", roleName), strings.Join(notReady, "; ")
}

func enabledDefaultTrue(value *bool) bool {
	return value == nil || *value
}

func degradedCondition(
	discovery domain.DiscoveryResult,
	plan planner.Output,
	monitorErr string,
	backendHealthyStatus metav1.ConditionStatus,
	backendHealthyMessage string,
	writerReadyStatus metav1.ConditionStatus,
	writerReadyMessage string,
	readerReadyStatus metav1.ConditionStatus,
	readerReadyMessage string,
	roleMismatchMessage string,
) (metav1.ConditionStatus, string, string) {
	reasons := make([]string, 0)
	if !discovery.Trusted {
		reason := "discovery untrusted"
		if discovery.Reason != "" {
			reason = fmt.Sprintf("%s: %s", reason, discovery.Reason)
		}
		reasons = append(reasons, reason)
	}
	if monitorErr != "" {
		reasons = append(reasons, fmt.Sprintf("monitor failed: %s", monitorErr))
	}
	if plan.Frozen {
		reasons = append(reasons, "plan frozen")
	}
	if backendHealthyStatus != metav1.ConditionTrue {
		reason := "backend unhealthy"
		if backendHealthyMessage != "" {
			reason = fmt.Sprintf("%s: %s", reason, backendHealthyMessage)
		}
		reasons = append(reasons, reason)
	}
	if writerReadyStatus != metav1.ConditionTrue {
		reason := "writer not ready"
		if writerReadyMessage != "" {
			reason = fmt.Sprintf("%s: %s", reason, writerReadyMessage)
		}
		reasons = append(reasons, reason)
	}
	if readerReadyStatus != metav1.ConditionTrue {
		reason := "reader not ready"
		if readerReadyMessage != "" {
			reason = fmt.Sprintf("%s: %s", reason, readerReadyMessage)
		}
		reasons = append(reasons, reason)
	}
	if roleMismatchMessage != "" {
		reasons = append(reasons, fmt.Sprintf("role mismatch: %s", roleMismatchMessage))
	}
	reasons = append(reasons, degradedPlanReasons(plan.Reasons)...)
	if len(reasons) == 0 {
		return metav1.ConditionFalse, "Healthy", ""
	}
	return metav1.ConditionTrue, "Degraded", strings.Join(reasons, "; ")
}

func degradedPlanReasons(planReasons []string) []string {
	reasons := make([]string, 0, len(planReasons))
	for _, reason := range planReasons {
		if reason == "reader fallback to writer" {
			continue
		}
		reasons = append(reasons, reason)
	}
	return reasons
}

func roleMismatchMessage(plan planner.Output) string {
	messages := make([]string, 0)
	for _, instance := range activePlanInstances(plan.Instances) {
		if !strings.Contains(instance.Reason, "role mismatch") {
			continue
		}
		messages = append(messages, fmt.Sprintf("%s: %s", instance.Name, instance.Reason))
	}
	return strings.Join(messages, "; ")
}

func readerFallbackFromWriter(plan planner.Output) bool {
	activeInstances := activePlanInstances(plan.Instances)
	instances := make(map[string]domain.InstancePlan, len(activeInstances))
	for _, instance := range activeInstances {
		instances[instance.Name] = instance
	}
	for _, member := range plan.Membership.Reader {
		if instances[member].Role == v1alpha1.RoleWriter {
			return true
		}
	}
	return false
}

func conditionWithTransition(previous []metav1.Condition, conditionType string, status metav1.ConditionStatus, reason string, message string, now metav1.Time) metav1.Condition {
	condition := metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, LastTransitionTime: now}
	for _, old := range previous {
		if old.Type != conditionType {
			continue
		}
		if old.Status == status && old.Reason == reason && old.Message == message {
			condition.LastTransitionTime = old.LastTransitionTime
		}
		return condition
	}
	return condition
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneMissingInstances(in []v1alpha1.MissingInstanceStatus) []v1alpha1.MissingInstanceStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.MissingInstanceStatus, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].FirstMissingTime = cloneTimePtr(in[i].FirstMissingTime)
		out[i].LastMissingTime = cloneTimePtr(in[i].LastMissingTime)
	}
	return out
}

func cloneTime(value metav1.Time) *metav1.Time {
	out := value.DeepCopy()
	return out
}

func cloneTimePtr(value *metav1.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	return value.DeepCopy()
}

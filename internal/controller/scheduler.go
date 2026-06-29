package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/domain"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/planner"
)

type Scheduler struct {
	Client           client.Client
	APIReader        client.Reader
	Discovery        Discovery
	Monitor          Monitor
	Events           chan<- event.GenericEvent
	Namespace        string
	WatchName        string
	Tick             time.Duration
	DiscoveryWorkers int
	MonitorWorkers   int
	Log              logr.Logger

	discoveryJobs chan types.NamespacedName
	monitorJobs   chan types.NamespacedName
	inFlightMu    sync.Mutex
	inFlight      map[string]struct{}
}

func (s *Scheduler) Start(ctx context.Context) error {
	tick := s.tick()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	if s.Log.GetSink() == nil {
		s.Log = ctrl.Log.WithName("scheduler")
	}
	s.discoveryJobs = make(chan types.NamespacedName, 1024)
	s.monitorJobs = make(chan types.NamespacedName, 1024)
	s.inFlight = map[string]struct{}{}
	for i := 0; i < s.discoveryWorkers(); i++ {
		go s.discoveryWorker(ctx, i)
	}
	for i := 0; i < s.monitorWorkers(); i++ {
		go s.monitorWorker(ctx, i)
	}
	s.Log.Info("scheduler started",
		"namespace", s.Namespace,
		"watchName", s.WatchName,
		"tick", tick.String(),
		"discoveryWorkers", s.discoveryWorkers(),
		"monitorWorkers", s.monitorWorkers(),
	)
	s.logManagedResources(ctx)
	for {
		s.enqueueDueJobs(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) enqueueDue(ctx context.Context) {
	s.enqueueDueJobs(ctx)
}

func (s *Scheduler) logManagedResources(ctx context.Context) {
	if s.Client == nil || strings.TrimSpace(s.Namespace) == "" {
		return
	}
	list := &v1alpha1.PgBouncerAuroraList{}
	if err := s.Client.List(ctx, list, client.InNamespace(s.Namespace)); err != nil {
		s.Log.Error(err, "scheduler startup list failed")
		return
	}
	crs := make([]string, 0, len(list.Items))
	now := metav1.Now()
	discoveryDueCount := 0
	monitorDueCount := 0
	for i := range list.Items {
		resource := &list.Items[i]
		if !schedulerMatchesWatchName(s.WatchName, resource.Name) {
			continue
		}
		crs = append(crs, resource.Name)
		if discoveryDue(resource, now) {
			discoveryDueCount++
		}
		if monitorDue(resource, now) && len(resource.Status.LastKnownTopology.Instances) > 0 {
			monitorDueCount++
		}
	}
	sort.Strings(crs)
	s.Log.Info("managed CR scheduling started",
		"namespace", s.Namespace,
		"watchName", s.WatchName,
		"count", len(crs),
		"crs", crs,
		"discoveryDue", discoveryDueCount,
		"monitorDue", monitorDueCount,
	)
}

func (s *Scheduler) enqueueDueJobs(ctx context.Context) {
	if s.Client == nil || strings.TrimSpace(s.Namespace) == "" {
		return
	}
	list := &v1alpha1.PgBouncerAuroraList{}
	if err := s.Client.List(ctx, list, client.InNamespace(s.Namespace)); err != nil {
		s.Log.Error(err, "scheduler list failed")
		return
	}
	now := metav1.Now()
	for i := range list.Items {
		resource := &list.Items[i]
		if !schedulerMatchesWatchName(s.WatchName, resource.Name) {
			continue
		}
		key := types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}
		if discoveryDue(resource, now) {
			s.enqueueJob(ctx, "discovery", key, s.discoveryJobs)
		}
		if monitorDue(resource, now) && len(resource.Status.LastKnownTopology.Instances) > 0 {
			s.enqueueJob(ctx, "monitor", key, s.monitorJobs)
		}
	}
}

func (s *Scheduler) enqueueJob(ctx context.Context, kind string, key types.NamespacedName, jobs chan<- types.NamespacedName) {
	if !s.markInFlight(kind, key) {
		return
	}
	select {
	case jobs <- key:
		s.Log.V(1).Info("scheduled job", "kind", kind, "namespace", key.Namespace, "cr", key.Name)
	case <-ctx.Done():
		s.clearInFlight(kind, key)
	default:
		s.clearInFlight(kind, key)
		s.Log.Error(fmt.Errorf("scheduler %s job queue is full", kind), "scheduler job queue full", "kind", kind, "namespace", key.Namespace, "cr", key.Name)
	}
}

func (s *Scheduler) discoveryWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-s.discoveryJobs:
			s.runDiscoveryJob(ctx, id, key)
			s.clearInFlight("discovery", key)
		}
	}
}

func (s *Scheduler) monitorWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-s.monitorJobs:
			s.runMonitorJob(ctx, id, key)
			s.clearInFlight("monitor", key)
		}
	}
}

func (s *Scheduler) runDiscoveryJob(ctx context.Context, id int, key types.NamespacedName) {
	log := s.Log.WithValues("worker", id, "kind", "discovery", "namespace", key.Namespace, "cr", key.Name)
	startedAt := time.Now()
	if s.Discovery == nil {
		log.Info("discovery skipped: provider not configured")
		return
	}
	resource := &v1alpha1.PgBouncerAurora{}
	if err := s.getResource(ctx, key, resource); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "discovery get failed")
		}
		return
	}
	if !schedulerMatchesWatchName(s.WatchName, resource.Name) || !discoveryDue(resource, metav1.Now()) {
		return
	}
	now := metav1.Now()
	discovery, err := s.Discovery.Discover(logr.NewContext(ctx, log), resource)
	discoveryFailed := false
	if err == nil && !discovery.Trusted {
		discoveryFailed = true
		discovery = discoveryAfterFailureThreshold(resource, discovery)
	}
	if err != nil {
		discovery = domain.DiscoveryResult{Trusted: false, Reason: fmt.Sprintf("discovery errored: %v", err)}
		discoveryFailed = true
	}
	missing := missingInstancesFor(resource, discovery, true, now)
	if err := s.updateDiscoveryStatus(ctx, resource, discovery, missing, discoveryFailed, now); err != nil {
		log.Error(err, "discovery status update failed")
		return
	}
	if discoveryFailed {
		failureCount := nextDiscoveryFailureCount(resource, discoveryFailed)
		threshold := discoveryFailureThreshold(resource)
		message := "discovery failed"
		if discovery.Trusted {
			message = "transient discovery failure; using cached topology"
		} else if failureCount >= threshold || len(resource.Status.LastKnownTopology.Instances) == 0 {
			message = "discovery failure threshold reached; topology will freeze"
		}
		log.Error(logReasonError(discovery.Reason), message,
			"trusted", discovery.Trusted,
			"cached", discovery.Trusted,
			"failureCount", failureCount,
			"threshold", threshold,
			"reason", truncateLogValue(discovery.Reason),
		)
	}
	if discovery.Trusted && topologyChanged(discovery.Instances, resource.Status.LastKnownTopology.Instances) {
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
	log.V(1).Info("discovery completed",
		"duration", time.Since(startedAt).String(),
		"trusted", discovery.Trusted,
		"failed", discoveryFailed,
		"reason", truncateLogValue(discovery.Reason),
		"instances", len(discovery.Instances),
		"writer", countDiscoveredRole(discovery.Instances, domain.RoleWriter),
		"reader", countDiscoveredRole(discovery.Instances, domain.RoleReader),
	)
	s.enqueueReconcile(ctx, key)
}

func (s *Scheduler) runMonitorJob(ctx context.Context, id int, key types.NamespacedName) {
	log := s.Log.WithValues("worker", id, "kind", "monitor", "namespace", key.Namespace, "cr", key.Name)
	startedAt := time.Now()
	if s.Monitor == nil {
		log.V(1).Info("monitor skipped: provider not configured")
		return
	}
	resource := &v1alpha1.PgBouncerAurora{}
	if err := s.getResource(ctx, key, resource); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "monitor get failed")
		}
		return
	}
	if !schedulerMatchesWatchName(s.WatchName, resource.Name) || !monitorDue(resource, metav1.Now()) {
		return
	}
	discovery := cachedDiscovery(resource)
	if !discovery.Trusted {
		return
	}
	now := metav1.Now()
	health, err := s.Monitor.Check(ctx, resource, discovery.Instances)
	monitorErr := ""
	if err != nil {
		health = healthFromStatus(resource.Status.Instances)
		monitorErr = err.Error()
	}
	plan := planner.Plan(planner.Input{
		Resource:         resource,
		DiscoveryTrusted: discovery.Trusted,
		Discovered:       discovery.Instances,
		Health:           health,
		CachedHealth:     err != nil,
		MissingInstances: cloneMissingInstances(resource.Status.MissingInstances),
	})
	if err := s.updateMonitorStatus(ctx, resource, discovery, plan, monitorErr, now); err != nil {
		log.Error(err, "monitor status update failed")
		return
	}
	unhealthy := countUnhealthy(health)
	if monitorErr != "" {
		log.Error(logReasonError(monitorErr), "monitor failed; using cached health",
			"cached", true,
			"healthy", countHealthy(health),
			"unhealthy", unhealthy,
			"instances", len(health),
			"error", truncateLogValue(monitorErr),
		)
	} else if unhealthy > 0 {
		log.Error(fmt.Errorf("%d unhealthy backend(s)", unhealthy), "monitor reported unhealthy backends",
			"healthy", countHealthy(health),
			"unhealthy", unhealthy,
			"instances", len(health),
		)
	}
	if monitorErr == "" {
		if diff := healthDiffForLog(resource.Status.Instances, plan.Instances); len(diff.Changed) > 0 {
			log.Info("backend health changed",
				"changed", diff.Changed,
				"healthy", countHealthy(health),
				"unhealthy", unhealthy,
				"instances", len(health),
			)
		}
	}
	log.V(1).Info("monitor completed",
		"duration", time.Since(startedAt).String(),
		"error", truncateLogValue(monitorErr),
		"healthy", countHealthy(health),
		"unhealthy", unhealthy,
		"instances", len(health),
	)
	s.enqueueReconcile(ctx, key)
}

func (s *Scheduler) updateDiscoveryStatus(ctx context.Context, resource *v1alpha1.PgBouncerAurora, discovery domain.DiscoveryResult, missing []v1alpha1.MissingInstanceStatus, discoveryFailed bool, now metav1.Time) error {
	key := types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1alpha1.PgBouncerAurora{}
		if err := s.getResource(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.LastDiscoveryTime != nil && fresh.Status.LastDiscoveryTime.Time.After(now.Time) {
			return nil
		}
		beforeStatus := hashObject(fresh.Status)
		fresh.Status.LastDiscoveryTime = &now
		if discoveryFailed {
			fresh.Status.ConsecutiveDiscoveryFailures++
		} else {
			fresh.Status.ConsecutiveDiscoveryFailures = 0
		}
		if discovery.Trusted {
			fresh.Status.LastKnownTopology.Instances = topologyStatus(discovery.Instances)
			fresh.Status.MissingInstances = missing
			fresh.Status.TopologyHash = hashObject(fresh.Status.LastKnownTopology.Instances)
		}
		fresh.Status.Conditions = discoveryOnlyConditions(fresh.Status.Conditions, discovery, discoveryFailed, now)
		if beforeStatus == hashObject(fresh.Status) {
			return nil
		}
		return s.Client.Status().Update(ctx, fresh)
	})
}

func (s *Scheduler) updateMonitorStatus(ctx context.Context, resource *v1alpha1.PgBouncerAurora, discovery domain.DiscoveryResult, plan planner.Output, monitorErr string, now metav1.Time) error {
	key := types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}
	monitorTopologyHash := hashObject(topologyStatus(discovery.Instances))
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1alpha1.PgBouncerAurora{}
		if err := s.getResource(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Status.LastMonitorTime != nil && fresh.Status.LastMonitorTime.Time.After(now.Time) {
			return nil
		}
		if fresh.Status.TopologyHash != "" && monitorTopologyHash != "" && fresh.Status.TopologyHash != monitorTopologyHash {
			return nil
		}
		beforeStatus := hashObject(fresh.Status)
		previousConditions := fresh.Status.Conditions
		fresh.Status.LastMonitorTime = &now
		fresh.Status.Instances = instanceStatus(plan.Instances)
		fresh.Status.ServiceSummary = serviceSummaryStatus(fresh, plan)
		fresh.Status.Conditions = conditionsFor(fresh, previousConditions, discovery, plan, monitorErr, now)
		if beforeStatus == hashObject(fresh.Status) {
			return nil
		}
		return s.Client.Status().Update(ctx, fresh)
	})
}

func (s *Scheduler) getResource(ctx context.Context, key types.NamespacedName, resource *v1alpha1.PgBouncerAurora) error {
	if s.APIReader != nil {
		return s.APIReader.Get(ctx, key, resource)
	}
	return s.Client.Get(ctx, key, resource)
}

func (s *Scheduler) enqueueReconcile(ctx context.Context, key types.NamespacedName) {
	if s.Events == nil {
		return
	}
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
	select {
	case s.Events <- event.GenericEvent{Object: resource}:
	case <-ctx.Done():
	default:
		s.Log.Error(fmt.Errorf("reconcile event queue is full"), "reconcile event queue full", "namespace", key.Namespace, "cr", key.Name)
	}
}

func (s *Scheduler) markInFlight(kind string, key types.NamespacedName) bool {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	if s.inFlight == nil {
		s.inFlight = map[string]struct{}{}
	}
	id := inFlightID(kind, key)
	if _, ok := s.inFlight[id]; ok {
		return false
	}
	s.inFlight[id] = struct{}{}
	return true
}

func (s *Scheduler) clearInFlight(kind string, key types.NamespacedName) {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	delete(s.inFlight, inFlightID(kind, key))
}

func inFlightID(kind string, key types.NamespacedName) string {
	return kind + ":" + key.String()
}

func (s *Scheduler) tick() time.Duration {
	if s.Tick > 0 {
		return s.Tick
	}
	return time.Second
}

func schedulerDue(resource *v1alpha1.PgBouncerAurora, now metav1.Time) bool {
	return discoveryDue(resource, now) || monitorDue(resource, now)
}

func schedulerMatchesWatchName(watchName string, name string) bool {
	watchName = strings.TrimSpace(watchName)
	return watchName == "" || watchName == "*" || watchName == name
}

func (s *Scheduler) discoveryWorkers() int {
	if s.DiscoveryWorkers > 0 {
		return s.DiscoveryWorkers
	}
	return 2
}

func (s *Scheduler) monitorWorkers() int {
	if s.MonitorWorkers > 0 {
		return s.MonitorWorkers
	}
	return 4
}

func cachedDiscovery(resource *v1alpha1.PgBouncerAurora) domain.DiscoveryResult {
	if !lastDiscoveryTrusted(resource) || len(resource.Status.LastKnownTopology.Instances) == 0 {
		return domain.DiscoveryResult{Trusted: false, Reason: lastDiscoveryMessage(resource)}
	}
	return domain.DiscoveryResult{
		Trusted:   true,
		Instances: topologyObservations(resource.Status.LastKnownTopology.Instances),
		Reason:    lastDiscoveryMessage(resource),
	}
}

func discoveryOnlyConditions(previous []metav1.Condition, discovery domain.DiscoveryResult, discoveryFailed bool, now metav1.Time) []metav1.Condition {
	status := metav1.ConditionFalse
	reason := "DiscoveryUntrusted"
	if discovery.Trusted {
		status = metav1.ConditionTrue
		reason = "DiscoveryTrusted"
	}
	if discoveryFailed && !discovery.Trusted {
		reason = "DiscoveryFailed"
	}
	out := append([]metav1.Condition{}, previous...)
	condition := conditionWithTransition(previous, "DiscoveryTrusted", status, reason, discovery.Reason, now)
	replaced := false
	for i := range out {
		if out[i].Type == condition.Type {
			out[i] = condition
			replaced = true
			break
		}
	}
	if !replaced {
		out = append(out, condition)
	}
	return out
}

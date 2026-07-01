package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"github.com/case-88/pgbouncer-aurora-operator/internal/planner"
	"github.com/case-88/pgbouncer-aurora-operator/internal/render"
)

func TestSchedulerEnqueuesDueResources(t *testing.T) {
	scheme := testScheme(t)
	due := sampleResource()
	due.Name = "due"
	due.Status.ObservedGeneration = due.Generation
	old := metav1.NewTime(time.Now().Add(-time.Minute))
	due.Status.LastDiscoveryTime = &old
	due.Status.LastMonitorTime = &old

	notDue := sampleResource()
	notDue.Name = "not-due"
	notDue.Status.ObservedGeneration = notDue.Generation
	now := metav1.Now()
	notDue.Status.LastDiscoveryTime = &now
	notDue.Status.LastMonitorTime = &now
	notDue.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleWriter}}
	notDue.Status.Instances = []v1alpha1.InstanceStatus{{InstanceName: "db-1", Healthy: true}}

	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(due, notDue).Build()
	events := make(chan event.GenericEvent, 4)
	scheduler := Scheduler{
		Client: c,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Events:    events,
		Namespace: due.Namespace,
		WatchName: "*",
	}
	scheduler.enqueueDue(context.Background())

	requireEvent(t, events, "due")
}

func TestDiscoveryDueRespectsIntervalAfterInitialFailure(t *testing.T) {
	now := metav1.Now()
	resource := sampleResource()
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.LastDiscoveryTime = &now
	resource.Status.LastKnownTopology.Instances = nil

	if discoveryDue(resource, now) {
		t.Fatalf("discovery should wait for interval after failed initial discovery")
	}

	stale := metav1.Time{Time: now.Time.Add(-discoveryInterval(resource))}
	resource.Status.LastDiscoveryTime = &stale
	if !discoveryDue(resource, now) {
		t.Fatalf("discovery should be due after interval")
	}
}

func TestSchedulerMonitorStatusSkipsStaleTopologySnapshot(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	current := sampleResource()
	oldTopology := []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-old", Endpoint: "db-old.example", Port: 5432, Role: v1alpha1.RoleWriter}}
	newTopology := []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-new", Endpoint: "db-new.example", Port: 5432, Role: v1alpha1.RoleWriter}}
	current.Status.LastKnownTopology.Instances = newTopology
	current.Status.TopologyHash = hashObject(newTopology)
	current.Status.Instances = []v1alpha1.InstanceStatus{{InstanceName: "db-new", Healthy: true}}
	oldSnapshot := current.DeepCopy()
	oldSnapshot.Status.LastKnownTopology.Instances = oldTopology
	oldSnapshot.Status.TopologyHash = hashObject(oldTopology)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(current).Build()
	scheduler := Scheduler{Client: c}
	oldDiscovery := domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{Name: "db-old", Endpoint: "db-old.example", Port: 5432, Role: domain.RoleWriter}}}
	oldPlan := planner.Output{Instances: []domain.InstancePlan{{InstanceObservation: oldDiscovery.Instances[0], Healthy: true}}}

	if err := scheduler.updateMonitorStatus(ctx, oldSnapshot, oldDiscovery, oldPlan, "", metav1.Now()); err != nil {
		t.Fatal(err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: current.Name, Namespace: current.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Instances) != 1 || updated.Status.Instances[0].InstanceName != "db-new" {
		t.Fatalf("stale monitor update overwrote current status: %#v", updated.Status.Instances)
	}
	if updated.Status.LastMonitorTime != nil {
		t.Fatalf("stale monitor update should not refresh LastMonitorTime: %#v", updated.Status.LastMonitorTime)
	}
}

func TestSchedulerMonitorStatusSkipsOlderJobSnapshot(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	current := sampleResource()
	current.Status.LastMonitorTime = &metav1.Time{Time: time.Now()}
	current.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleWriter}}
	current.Status.TopologyHash = hashObject(current.Status.LastKnownTopology.Instances)
	current.Status.Instances = []v1alpha1.InstanceStatus{{InstanceName: "db-1", Healthy: true, Reason: "new"}}
	oldSnapshot := current.DeepCopy()
	oldDiscovery := domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}}}
	oldPlan := planner.Output{Instances: []domain.InstancePlan{{InstanceObservation: oldDiscovery.Instances[0], Healthy: false, Reason: "old"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(current).Build()
	scheduler := Scheduler{Client: c}

	if err := scheduler.updateMonitorStatus(ctx, oldSnapshot, oldDiscovery, oldPlan, "", metav1.Time{Time: current.Status.LastMonitorTime.Time.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: current.Name, Namespace: current.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.Instances) != 1 || !updated.Status.Instances[0].Healthy || updated.Status.Instances[0].Reason != "new" {
		t.Fatalf("older monitor job overwrote newer health: %#v", updated.Status.Instances)
	}
}

func TestSchedulerDiscoveryStatusSkipsOlderJobSnapshot(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	current := sampleResource()
	current.Status.LastDiscoveryTime = &metav1.Time{Time: time.Now()}
	current.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-new", Endpoint: "db-new.example", Port: 5432, Role: v1alpha1.RoleWriter}}
	current.Status.TopologyHash = hashObject(current.Status.LastKnownTopology.Instances)
	oldDiscovery := domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{Name: "db-old", Endpoint: "db-old.example", Port: 5432, Role: domain.RoleWriter}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(current).Build()
	scheduler := Scheduler{Client: c}

	if err := scheduler.updateDiscoveryStatus(ctx, current.DeepCopy(), oldDiscovery, nil, false, metav1.Time{Time: current.Status.LastDiscoveryTime.Time.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: current.Name, Namespace: current.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.LastKnownTopology.Instances) != 1 || updated.Status.LastKnownTopology.Instances[0].InstanceName != "db-new" {
		t.Fatalf("older discovery job overwrote newer topology: %#v", updated.Status.LastKnownTopology.Instances)
	}
}

func TestSchedulerHonorsWatchName(t *testing.T) {
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Name = "target"
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	events := make(chan event.GenericEvent, 4)
	scheduler := Scheduler{
		Client: c,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Events:    events,
		Namespace: resource.Namespace,
		WatchName: "other",
	}
	scheduler.enqueueDue(context.Background())
	if len(scheduler.inFlight) != 0 {
		t.Fatalf("watch-names mismatch should not schedule discovery")
	}
	scheduler.WatchName = "other,target"
	scheduler.enqueueDue(context.Background())
	requireEvent(t, events, "target")
}

func TestSchedulerDiscoveryJobUpdatesTopologyAndEnqueuesReconcile(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	events := make(chan event.GenericEvent, 4)
	scheduler := Scheduler{
		Client: c,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
		}}}},
		Events: events,
	}

	scheduler.runDiscoveryJob(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace})

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.LastDiscoveryTime == nil || len(updated.Status.LastKnownTopology.Instances) != 1 {
		t.Fatalf("discovery status not updated: %#v", updated.Status)
	}
	if updated.Status.LastKnownTopology.Instances[0].InstanceName != "db-1" {
		t.Fatalf("topology = %#v", updated.Status.LastKnownTopology.Instances)
	}
	if len(events) != 1 {
		t.Fatalf("expected reconcile event, got %d", len(events))
	}
}

func TestSchedulerDiscoveryJobCountsCachedDiscoveryFailure(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Discovery.FailureThreshold = 3
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.ConsecutiveDiscoveryFailures = 1
	resource.Status.LastDiscoveryTime = &metav1.Time{Time: time.Now().Add(-discoveryInterval(resource))}
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName:     "old-db",
		Endpoint:         "old-db.example",
		Port:             5432,
		Role:             v1alpha1.RoleWriter,
		AvailabilityZone: "ap-northeast-2a",
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	events := make(chan event.GenericEvent, 4)
	scheduler := Scheduler{
		Client:    c,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: false, Reason: "secret not found"}},
		Events:    events,
	}

	scheduler.runDiscoveryJob(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace})

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.ConsecutiveDiscoveryFailures != 2 {
		t.Fatalf("discovery failures = %d", updated.Status.ConsecutiveDiscoveryFailures)
	}
	if !hasCondition(updated.Status.Conditions, "DiscoveryTrusted", metav1.ConditionTrue) {
		t.Fatalf("cached discovery should remain trusted before threshold: %#v", updated.Status.Conditions)
	}
	if len(updated.Status.LastKnownTopology.Instances) != 1 || updated.Status.LastKnownTopology.Instances[0].InstanceName != "old-db" {
		t.Fatalf("cached topology not preserved: %#v", updated.Status.LastKnownTopology.Instances)
	}
	if len(events) != 1 {
		t.Fatalf("expected reconcile event, got %d", len(events))
	}
}

func TestSchedulerMonitorJobUpdatesHealthAndEnqueuesReconcile(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.ObservedGeneration = resource.Generation
	now := metav1.Now()
	resource.Status.LastDiscoveryTime = &now
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue, Reason: "DiscoveryTrusted"}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: resource.Namespace, Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   render.ClusterLabelValue(resource.Name),
			render.LabelInstance:  "db-1",
		}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource, pod).Build()
	events := make(chan event.GenericEvent, 4)
	scheduler := Scheduler{
		Client:  c,
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true, Reason: "healthy", ReadyReplicas: 1}}},
		Events:  events,
	}

	scheduler.runMonitorJob(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace})

	updated := &v1alpha1.PgBouncerAurora{}
	if err := c.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.LastMonitorTime == nil || len(updated.Status.Instances) != 1 {
		t.Fatalf("monitor status not updated: %#v", updated.Status)
	}
	if !updated.Status.Instances[0].Healthy || updated.Status.Instances[0].ReadyReplicas != 1 {
		t.Fatalf("instance health = %#v", updated.Status.Instances[0])
	}
	if len(events) != 1 {
		t.Fatalf("expected reconcile event, got %d", len(events))
	}
}

func requireEvent(t *testing.T, events <-chan event.GenericEvent, name string) {
	t.Helper()
	select {
	case got := <-events:
		if got.Object.GetName() != name {
			t.Fatalf("event object = %s, want %s", got.Object.GetName(), name)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for reconcile event for %s", name)
	}
}

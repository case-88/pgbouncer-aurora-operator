package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"github.com/case-88/pgbouncer-aurora-operator/internal/planner"
	"github.com/case-88/pgbouncer-aurora-operator/internal/render"
)

type fakeDiscovery struct {
	result domain.DiscoveryResult
	err    error
}

func (f fakeDiscovery) Discover(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (domain.DiscoveryResult, error) {
	return f.result, f.err
}

type fakeMonitor struct {
	health map[string]domain.HealthStatus
	err    error
}

func (f fakeMonitor) Check(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) (map[string]domain.HealthStatus, error) {
	return f.health, f.err
}

type countingDiscovery struct {
	calls  int
	result domain.DiscoveryResult
}

func (f *countingDiscovery) Discover(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (domain.DiscoveryResult, error) {
	f.calls++
	return f.result, nil
}

type countingMonitor struct {
	calls  int
	health map[string]domain.HealthStatus
}

func (f *countingMonitor) Check(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) (map[string]domain.HealthStatus, error) {
	f.calls++
	return f.health, nil
}

type recordingMonitor struct {
	instances []domain.InstanceObservation
	health    map[string]domain.HealthStatus
}

func (f *recordingMonitor) Check(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) (map[string]domain.HealthStatus, error) {
	f.instances = append([]domain.InstanceObservation(nil), instances...)
	return f.health, nil
}

func TestReconcileCreatesInstanceAndRoleResources(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a"},
			{Name: "db-2", Endpoint: "db-2.example", Port: 5432, Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
		}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{
			"db-1": {Healthy: true, ReadyReplicas: 1},
			"db-2": {Healthy: true, ReadyReplicas: 1},
		}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-1")
	assertExists[*corev1.Service](t, ctx, client, "sample-db-1")
	writer := assertExists[*corev1.Service](t, ctx, client, "sample-writer")
	reader := assertExists[*corev1.Service](t, ctx, client, "sample-reader")

	if writer.Spec.Selector[render.LabelWriter] != "true" {
		t.Fatalf("writer selector mismatch: %#v", writer.Spec.Selector)
	}
	if reader.Spec.Selector[render.LabelReader] != "true" {
		t.Fatalf("reader selector mismatch: %#v", reader.Spec.Selector)
	}

	deployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-1")
	if _, ok := deployment.Spec.Template.Labels[render.LabelWriter]; ok {
		t.Fatalf("deployment template must not contain writer membership label: %#v", deployment.Spec.Template.Labels)
	}
	preferred := deployment.Spec.Template.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(preferred) != 1 || preferred[0].Preference.MatchExpressions[0].Values[0] != "ap-northeast-2a" {
		t.Fatalf("zone affinity mismatch: %#v", preferred)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-1" {
		t.Fatalf("writer status mismatch: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	if updated.Status.TopologyHash == "" || updated.Status.MembershipHash == "" {
		t.Fatalf("status hashes should be set: topology=%q membership=%q", updated.Status.TopologyHash, updated.Status.MembershipHash)
	}
	if updated.Status.Instances[0].ConsecutiveSuccesses != 1 || updated.Status.Instances[0].ConsecutiveFailures != 0 {
		t.Fatalf("instance monitor counters mismatch: %#v", updated.Status.Instances[0])
	}
	if updated.Status.ServiceSummary.Writer.ServiceName != "sample-writer" ||
		updated.Status.ServiceSummary.Writer.TotalCandidates != 1 ||
		updated.Status.ServiceSummary.Writer.Healthy != 1 ||
		updated.Status.ServiceSummary.Writer.Members != 1 {
		t.Fatalf("writer service summary mismatch: %#v", updated.Status.ServiceSummary.Writer)
	}
	if updated.Status.ServiceSummary.Reader.ServiceName != "sample-reader" ||
		updated.Status.ServiceSummary.Reader.TotalCandidates != 1 ||
		updated.Status.ServiceSummary.Reader.Healthy != 1 ||
		updated.Status.ServiceSummary.Reader.Members != 1 {
		t.Fatalf("reader service summary mismatch: %#v", updated.Status.ServiceSummary.Reader)
	}
	for _, condition := range []struct {
		typ    string
		status metav1.ConditionStatus
	}{
		{typ: "BackendHealthy", status: metav1.ConditionTrue},
		{typ: "WriterReady", status: metav1.ConditionTrue},
		{typ: "ReaderReady", status: metav1.ConditionTrue},
		{typ: "Frozen", status: metav1.ConditionFalse},
		{typ: "Degraded", status: metav1.ConditionFalse},
	} {
		if !hasCondition(updated.Status.Conditions, condition.typ, condition.status) {
			t.Fatalf("%s=%s condition missing: %#v", condition.typ, condition.status, updated.Status.Conditions)
		}
	}
}

func TestReconcileSkipsApplyWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	applyGets := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *corev1.ConfigMap, *corev1.Service, *appsv1.Deployment:
					applyGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
		{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a"},
		{Name: "db-2", Endpoint: "db-2.example", Port: 5432, Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
	}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{
		"db-1": {Healthy: true},
		"db-2": {Healthy: true},
	}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	applyGets = 0
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if applyGets != 0 {
		t.Fatalf("cached reconcile should skip desired object GETs, got %d", applyGets)
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileSkipsApplyWhenChecksDueButDesiredStateUnchanged(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Discovery.Interval = metav1.Duration{Duration: time.Second}
	resource.Spec.Monitor.Interval = metav1.Duration{Duration: time.Second}
	applyGets := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *corev1.ConfigMap, *corev1.Service, *appsv1.Deployment:
					applyGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	updated.Status.LastDiscoveryTime = &past
	updated.Status.LastMonitorTime = &past
	if err := client.Status().Update(ctx, updated); err != nil {
		t.Fatalf("status update failed: %v", err)
	}
	applyGets = 0
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if discovery.calls != 1 || monitor.calls != 1 {
		t.Fatalf("checks should run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
	if applyGets != 0 {
		t.Fatalf("unchanged due checks should skip desired object GETs, got %d", applyGets)
	}
}

func TestReconcileRollsDeploymentOnAuthFileSecretResourceVersionChange(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            resource.Spec.PgBouncer.AuthFileSecretRef.Name,
			Namespace:       resource.Namespace,
			UID:             types.UID("secret-uid"),
			ResourceVersion: "1",
		},
		Data: map[string][]byte{"userlist.txt": []byte(`"svc" "md5-old"`)},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource, secret).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	deployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-1")
	firstHash := deployment.Spec.Template.Annotations[render.AnnotationAuthFileHash]
	if firstHash != authFileMetadataHash(secret) {
		t.Fatalf("auth hash mismatch: got=%q want=%q", firstHash, authFileMetadataHash(secret))
	}

	secret.Data["userlist.txt"] = []byte(`"svc" "md5-new"`)
	if err := client.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	deployment = assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-1")
	if deployment.Spec.Template.Annotations[render.AnnotationAuthFileHash] == firstHash {
		t.Fatalf("auth file hash should change during reconcile: %s", firstHash)
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("secret-only drift should not require expensive checks: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileRestoresMissingOwnedResourceWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if err := client.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sample-db-1", Namespace: resource.Namespace}}); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileRepairsConfigMapDriftWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	cm := assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	cm.Data["pgbouncer.ini"] = "broken"
	if err := client.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	updated := assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	if updated.Data["pgbouncer.ini"] == "broken" {
		t.Fatalf("configmap drift not repaired")
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileRepairsServiceDriftWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	service := assertExists[*corev1.Service](t, ctx, client, "sample-writer")
	service.Spec.Selector[render.LabelWriter] = "broken"
	if err := client.Update(ctx, service); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	updated := assertExists[*corev1.Service](t, ctx, client, "sample-writer")
	if updated.Spec.Selector[render.LabelWriter] != "true" {
		t.Fatalf("service drift not repaired: %#v", updated.Spec.Selector)
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileDeletesStaleRoleServiceWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	stale := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "sample-old-writer",
		Namespace: resource.Namespace,
		Labels: map[string]string{
			render.LabelManagedBy:   render.ManagedByValue,
			render.LabelCluster:     render.ClusterLabelValue(resource.Name),
			render.LabelServiceRole: string(v1alpha1.RoleWriter),
		},
	}}
	if err := client.Create(ctx, stale); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	assertNotFound[*corev1.Service](t, ctx, client, stale.Name)
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestReconcileRepairsPodMembershipDriftWhenChecksNotDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true, ReadyReplicas: 1}}}
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	pod := readyManagedPod(resource, "pod-db-1", "db-1")
	pod.Labels[render.LabelWriter] = "true"
	pod.Labels[render.LabelRole] = string(v1alpha1.RoleWriter)
	if err := client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Labels[render.LabelWriter] = ""
	delete(pod.Labels, render.LabelRole)
	if err := client.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	discovery.calls = 0
	monitor.calls = 0

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	updated := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Labels[render.LabelWriter] != "true" || updated.Labels[render.LabelRole] != string(v1alpha1.RoleWriter) {
		t.Fatalf("membership labels not repaired: %#v", updated.Labels)
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("checks should not run: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestPatchPodMembershipAddsBeforeRemoving(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	oldPod := readyManagedPod(resource, "old-reader", "db-old")
	oldPod.Labels[render.LabelReader] = "true"
	newPod := readyManagedPod(resource, "new-reader", "db-new")
	var patchOrder []string
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(resource, oldPod, newPod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if pod, ok := obj.(*corev1.Pod); ok {
					if pod.Name == "new-reader" && pod.Labels[render.LabelReader] == "true" {
						patchOrder = append(patchOrder, "add-new")
					}
					if pod.Name == "old-reader" && pod.Labels[render.LabelReader] == "" {
						patchOrder = append(patchOrder, "remove-old")
					}
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	plan := planner.Output{
		Instances: []domain.InstancePlan{{
			InstanceObservation: domain.InstanceObservation{Name: "db-old", Role: v1alpha1.RoleReader},
		}, {
			InstanceObservation: domain.InstanceObservation{Name: "db-new", Role: v1alpha1.RoleReader},
		}},
		Membership: domain.ServiceMembership{Reader: []string{"db-new"}},
	}

	if err := reconciler.patchPodMembership(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}
	if len(patchOrder) != 2 || patchOrder[0] != "add-new" || patchOrder[1] != "remove-old" {
		t.Fatalf("membership patch order = %#v", patchOrder)
	}
}

func TestPatchPodMembershipRetriesConflict(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	pod := readyManagedPod(resource, "reader", "db-1")
	pod.Labels[render.LabelReader] = "true"
	patchCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(resource, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				patchCount++
				if patchCount == 1 {
					return apierrors.NewConflict(
						schema.GroupResource{Resource: "pods"},
						obj.GetName(),
						errors.New("pod conflict"),
					)
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	plan := planner.Output{
		Instances: []domain.InstancePlan{{
			InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
		}},
		Membership: domain.ServiceMembership{Writer: []string{"db-1"}},
	}

	if err := reconciler.patchPodMembership(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}
	if patchCount != 3 {
		t.Fatalf("patch count = %d", patchCount)
	}
	updated := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Labels[render.LabelWriter] != "true" || updated.Labels[render.LabelReader] != "" {
		t.Fatalf("membership labels not reconciled: %#v", updated.Labels)
	}
}

func TestPatchPodMembershipIgnoresNotFoundDuringPatch(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()

	t.Run("additions pass", func(t *testing.T) {
		pod := readyManagedPod(resource, "reader", "db-1")
		patchCount := 0
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(resource, pod).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patchCount++
					return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, obj.GetName())
				},
			}).
			Build()
		reconciler := &PgBouncerAuroraReconciler{Client: k8sClient, Scheme: scheme}
		plan := planner.Output{
			Instances: []domain.InstancePlan{{
				InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
			}},
			Membership: domain.ServiceMembership{Writer: []string{"db-1"}},
		}

		if err := reconciler.patchPodMembership(ctx, resource, plan); err != nil {
			t.Fatal(err)
		}
		if patchCount != 1 {
			t.Fatalf("patch count = %d", patchCount)
		}
	})

	t.Run("exact pass", func(t *testing.T) {
		pod := readyManagedPod(resource, "writer", "db-1")
		pod.Labels[render.LabelWriter] = "true"
		pod.Labels[render.LabelReader] = ""
		pod.Labels[render.LabelRole] = string(v1alpha1.RoleWriter)
		patchCount := 0
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(resource, pod).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patchCount++
					return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, obj.GetName())
				},
			}).
			Build()
		reconciler := &PgBouncerAuroraReconciler{Client: k8sClient, Scheme: scheme}
		plan := planner.Output{
			Instances: []domain.InstancePlan{{
				InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
			}},
			Membership: domain.ServiceMembership{Writer: []string{"db-1"}},
		}

		if err := reconciler.patchPodMembership(ctx, resource, plan); err != nil {
			t.Fatal(err)
		}
		if patchCount != 1 {
			t.Fatalf("patch count = %d", patchCount)
		}
	})
}

func TestPatchPodMembershipRemovesPresentEmptyLabels(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	pod := readyManagedPod(resource, "pod-db-1", "db-1")
	pod.Labels[render.LabelWriter] = ""
	pod.Labels[render.LabelReader] = ""
	pod.Labels[render.LabelRole] = ""
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource, pod).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: k8sClient, Scheme: scheme}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1"},
	}}}

	if err := reconciler.patchPodMembership(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}
	updated := &corev1.Pod{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{render.LabelWriter, render.LabelReader, render.LabelRole} {
		if _, ok := updated.Labels[label]; ok {
			t.Fatalf("empty membership label %q should be removed: %#v", label, updated.Labels)
		}
	}
}

func TestUpdateStatusRetriesConflict(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	statusUpdates := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" {
					statusUpdates++
					if statusUpdates == 1 {
						return apierrors.NewConflict(
							schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "pgbouncerauroras"},
							obj.GetName(),
							errors.New("status conflict"),
						)
					}
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	discovery := domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
	}}}
	plan := planner.Output{
		Instances: []domain.InstancePlan{{
			InstanceObservation: discovery.Instances[0],
			Healthy:             true,
			Reason:              "healthy",
			Replicas:            1,
			ReadyReplicas:       1,
		}},
		Membership: domain.ServiceMembership{Writer: []string{"db-1"}},
	}

	err := reconciler.updateStatus(ctx, resource, discovery, plan, nil, true, false, true, "", now)
	if err != nil {
		t.Fatalf("updateStatus failed: %v", err)
	}
	if statusUpdates != 2 {
		t.Fatalf("status update count = %d", statusUpdates)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-1" {
		t.Fatalf("writer status mismatch: %#v", updated.Status.LastAppliedMembership.Writer)
	}
}

func TestUpdateStatusDoesNotObserveNewerGeneration(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Generation = 2
	stored := resource.DeepCopy()
	stored.Generation = 3
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(stored).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	discovery := domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
	}}}
	plan := planner.Output{
		Instances: []domain.InstancePlan{{
			InstanceObservation: discovery.Instances[0],
			Healthy:             true,
			Reason:              "healthy",
			Replicas:            1,
			ReadyReplicas:       1,
		}},
		Membership: domain.ServiceMembership{Writer: []string{"db-1"}},
	}

	if err := reconciler.updateStatus(ctx, resource, discovery, plan, nil, true, false, true, "", now); err != nil {
		t.Fatalf("updateStatus failed: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if updated.Status.ObservedGeneration != 2 {
		t.Fatalf("observedGeneration should follow reconciled generation, got %d", updated.Status.ObservedGeneration)
	}
}

func TestReconcileSkipsExpensiveChecksUntilIntervalsAreDue(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	resource.Spec.Discovery.Interval = metav1.Duration{Duration: time.Hour}
	resource.Spec.Monitor.Interval = metav1.Duration{Duration: time.Hour}
	resource.Generation = 1
	resource.Status.ObservedGeneration = 1
	resource.Status.LastDiscoveryTime = &now
	resource.Status.LastMonitorTime = &now
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName:     "db-1",
		Endpoint:         "db-1.example",
		Port:             5432,
		Role:             v1alpha1.RoleWriter,
		AvailabilityZone: "ap-northeast-2a",
	}}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:         "db-1",
		Endpoint:             "db-1.example",
		Port:                 5432,
		Role:                 v1alpha1.RoleWriter,
		AvailabilityZone:     "ap-northeast-2a",
		Healthy:              true,
		ConsecutiveSuccesses: 7,
		Reason:               "healthy",
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: false, Reason: "should not be used"}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if discovery.calls != 0 || monitor.calls != 0 {
		t.Fatalf("expensive checks should be skipped, discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if updated.Status.Instances[0].ConsecutiveSuccesses != 7 {
		t.Fatalf("cached health counters should not advance: %#v", updated.Status.Instances[0])
	}
	if !updated.Status.LastDiscoveryTime.Time.Equal(now.Time) || !updated.Status.LastMonitorTime.Time.Equal(now.Time) {
		t.Fatalf("last check times should be preserved: discovery=%v monitor=%v", updated.Status.LastDiscoveryTime, updated.Status.LastMonitorTime)
	}
}

func TestReconcileRunsMonitorWhenPodReadinessChangesBeforeInterval(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	resource.Spec.Discovery.Interval = metav1.Duration{Duration: time.Hour}
	resource.Spec.Monitor.Interval = metav1.Duration{Duration: time.Hour}
	resource.Generation = 1
	resource.Status.ObservedGeneration = 1
	resource.Status.LastDiscoveryTime = &now
	resource.Status.LastMonitorTime = &now
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName: "db-1",
		Endpoint:     "db-1.example",
		Port:         5432,
		Role:         v1alpha1.RoleWriter,
	}}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:    "db-1",
		Endpoint:        "db-1.example",
		Port:            5432,
		Role:            v1alpha1.RoleWriter,
		Healthy:         false,
		ReadyReplicas:   0,
		DesiredReplicas: 1,
		Reason:          "pod not ready",
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: resource.Namespace,
			Labels: map[string]string{
				render.LabelManagedBy: render.ManagedByValue,
				render.LabelCluster:   render.ClusterLabelValue(resource.Name),
				render.LabelInstance:  "db-1",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}},
	}
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {
		Healthy:       true,
		ReadyReplicas: 1,
		Reason:        "healthy",
	}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource, pod).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme, Discovery: discovery, Monitor: monitor}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if discovery.calls != 0 || monitor.calls != 1 {
		t.Fatalf("pod readiness change should trigger monitor only, discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if !updated.Status.Instances[0].Healthy || updated.Status.Instances[0].ReadyReplicas != 1 {
		t.Fatalf("readiness-triggered monitor should refresh health: %#v", updated.Status.Instances[0])
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-1" {
		t.Fatalf("writer membership should be applied after pod ready: %#v", updated.Status.LastAppliedMembership.Writer)
	}
}

func TestRequeueAfterUsesDefaultAndMinimumIntervals(t *testing.T) {
	resource := sampleResource()
	if got := requeueAfter(resource); got != 3*time.Second {
		t.Fatalf("default requeue interval mismatch: %v", got)
	}

	resource.Spec.Discovery.Interval = metav1.Duration{Duration: 500 * time.Millisecond}
	resource.Spec.Monitor.Interval = metav1.Duration{Duration: 200 * time.Millisecond}
	if got := requeueAfter(resource); got != time.Second {
		t.Fatalf("minimum requeue interval mismatch: %v", got)
	}

	resource = sampleResource()
	resource.Spec.Discovery.Interval = metav1.Duration{Duration: time.Minute}
	if got := monitorInterval(resource); got != 10*time.Second {
		t.Fatalf("monitor default should not inherit discovery interval: %v", got)
	}
	if got := requeueAfter(resource); got != 10*time.Second {
		t.Fatalf("monitor default should keep reconcile responsive: %v", got)
	}

	resource.Spec.Discovery.Interval = metav1.Duration{Duration: time.Minute}
	resource.Spec.Monitor.Interval = metav1.Duration{Duration: 5 * time.Second}
	if got := requeueAfter(resource); got != 5*time.Second {
		t.Fatalf("shorter monitor interval should win: %v", got)
	}
}

func TestReconcileReportsReaderFallback(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if !updated.Status.ServiceSummary.Reader.FallbackFromWriter || updated.Status.ServiceSummary.Reader.Members != 1 {
		t.Fatalf("reader fallback summary mismatch: %#v", updated.Status.ServiceSummary.Reader)
	}
	if updated.Status.ServiceSummary.Reader.TotalCandidates != 1 ||
		updated.Status.ServiceSummary.Reader.Healthy != 1 ||
		updated.Status.ServiceSummary.Reader.Unhealthy != 0 {
		t.Fatalf("reader fallback health summary mismatch: %#v", updated.Status.ServiceSummary.Reader)
	}
	if !hasCondition(updated.Status.Conditions, "ReaderFallback", metav1.ConditionTrue) {
		t.Fatalf("ReaderFallback condition missing: %#v", updated.Status.Conditions)
	}
}

func TestReaderFallbackDoesNotDegradeCluster(t *testing.T) {
	plan := planner.Output{
		Instances: []domain.InstancePlan{{
			InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
			Healthy:             true,
			ReadyReplicas:       1,
		}},
		Membership: domain.ServiceMembership{
			Writer: []string{"db-1"},
			Reader: []string{"db-1"},
		},
		Reasons: []string{"reader fallback to writer"},
	}

	status, reason, message := degradedCondition(
		domain.DiscoveryResult{Trusted: true},
		plan,
		"",
		metav1.ConditionTrue,
		"",
		metav1.ConditionTrue,
		"",
		metav1.ConditionTrue,
		"",
		"",
	)
	if status != metav1.ConditionFalse || reason != "Healthy" || message != "" {
		t.Fatalf("reader fallback should not degrade cluster: status=%s reason=%s message=%q", status, reason, message)
	}
}

func TestReconcileReportsRoleMismatch(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Monitor.FailureThreshold = 1
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {
			Healthy: false,
			Reason:  "role mismatch: discovery=writer monitor=reader",
		}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if !hasCondition(updated.Status.Conditions, "RoleMismatch", metav1.ConditionTrue) {
		t.Fatalf("RoleMismatch condition missing: %#v", updated.Status.Conditions)
	}
}

func TestRoleServiceSummaryUsesRenderedServiceName(t *testing.T) {
	resource := sampleResource()
	resource.Name = "sample-pgbouncer-aurora-operator-cluster-name"
	resource.Spec.Services.Writer.Name = "writer"
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
		Healthy:             true,
	}}, Membership: domain.ServiceMembership{Writer: []string{"db-1"}}}

	summary := serviceSummaryStatus(resource, plan)
	if summary.Writer.ServiceName != render.RoleServiceName(resource.Name, "writer") {
		t.Fatalf("service name mismatch: %s", summary.Writer.ServiceName)
	}
	if len(summary.Writer.ServiceName) > 63 {
		t.Fatalf("service name too long: %s", summary.Writer.ServiceName)
	}
}

func TestReconcileDoesNotApplyWhenDiscoveryUntrusted(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.LastAppliedMembership.Writer = []string{"old-db"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:    "old-db",
		Endpoint:        "old-db.example",
		Port:            5432,
		Role:            v1alpha1.RoleWriter,
		Healthy:         true,
		DesiredReplicas: 1,
		ReadyReplicas:   1,
		Reason:          "healthy",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: false, Reason: "timeout", Instances: []domain.InstanceObservation{{
			Name: "new-db", Endpoint: "new-db.example", Role: domain.RoleWriter,
		}}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	service := &corev1.Service{}
	err = client.Get(ctx, types.NamespacedName{Name: "sample-new-db", Namespace: resource.Namespace}, service)
	if err == nil {
		t.Fatalf("unexpected service created during frozen reconcile")
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "old-db" {
		t.Fatalf("last applied membership should be preserved: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	if len(updated.Status.Instances) != 1 || updated.Status.Instances[0].InstanceName != "old-db" || updated.Status.ServiceSummary.Writer.Members != 1 {
		t.Fatalf("frozen status should preserve old instance summary: instances=%#v summary=%#v", updated.Status.Instances, updated.Status.ServiceSummary.Writer)
	}
	if !hasCondition(updated.Status.Conditions, "Frozen", metav1.ConditionTrue) {
		t.Fatalf("Frozen true condition missing: %#v", updated.Status.Conditions)
	}
	if !hasCondition(updated.Status.Conditions, "Reconciled", metav1.ConditionFalse) {
		t.Fatalf("Reconciled false condition missing: %#v", updated.Status.Conditions)
	}
	if !hasCondition(updated.Status.Conditions, "Degraded", metav1.ConditionTrue) {
		t.Fatalf("Degraded true condition missing: %#v", updated.Status.Conditions)
	}
}

func TestReconcileUsesCachedDiscoveryBeforeFailureThreshold(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Discovery.FailureThreshold = 3
	resource.Status.ConsecutiveDiscoveryFailures = 1
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName:     "old-db",
		Endpoint:         "old-db.example",
		Port:             5432,
		Role:             v1alpha1.RoleWriter,
		AvailabilityZone: "ap-northeast-2a",
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client:    client,
		Scheme:    scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: false, Reason: "timeout"}},
		Monitor:   fakeMonitor{health: map[string]domain.HealthStatus{"old-db": {Healthy: true, ReadyReplicas: 1}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	assertExists[*corev1.Service](t, ctx, client, "sample-old-db")
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if updated.Status.ConsecutiveDiscoveryFailures != 2 {
		t.Fatalf("discovery failures = %d", updated.Status.ConsecutiveDiscoveryFailures)
	}
	if !hasCondition(updated.Status.Conditions, "DiscoveryTrusted", metav1.ConditionTrue) {
		t.Fatalf("cached discovery should stay trusted before threshold: %#v", updated.Status.Conditions)
	}
	if hasCondition(updated.Status.Conditions, "Frozen", metav1.ConditionTrue) {
		t.Fatalf("plan should not freeze before threshold: %#v", updated.Status.Conditions)
	}
}

func TestReconcileFreezesAtDiscoveryFailureThreshold(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Discovery.FailureThreshold = 3
	resource.Status.ConsecutiveDiscoveryFailures = 2
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName: "old-db",
		Endpoint:     "old-db.example",
		Port:         5432,
		Role:         v1alpha1.RoleWriter,
	}}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client:    client,
		Scheme:    scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: false, Reason: "timeout"}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if updated.Status.ConsecutiveDiscoveryFailures != 3 {
		t.Fatalf("discovery failures = %d", updated.Status.ConsecutiveDiscoveryFailures)
	}
	if !hasCondition(updated.Status.Conditions, "Frozen", metav1.ConditionTrue) {
		t.Fatalf("plan should freeze at threshold: %#v", updated.Status.Conditions)
	}
}

func TestReconcileKeepsCachedHealthWhenMonitorFails(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.LastAppliedMembership.Writer = []string{"db-1"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:         "db-1",
		Endpoint:             "db-1.example",
		Port:                 5432,
		Role:                 v1alpha1.RoleWriter,
		Healthy:              true,
		ConsecutiveSuccesses: 3,
		Reason:               "healthy",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{err: errors.New("secret missing")},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile should not fail on monitor error: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-1" {
		t.Fatalf("writer membership should be preserved: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	if !hasCondition(updated.Status.Conditions, "MonitorSucceeded", metav1.ConditionFalse) {
		t.Fatalf("MonitorSucceeded false condition missing: %#v", updated.Status.Conditions)
	}
	if !hasCondition(updated.Status.Conditions, "Degraded", metav1.ConditionTrue) {
		t.Fatalf("Degraded true condition missing: %#v", updated.Status.Conditions)
	}
	if updated.Status.LastMonitorTime != nil {
		t.Fatalf("failed monitor should not advance LastMonitorTime: %v", updated.Status.LastMonitorTime)
	}
}

func TestReconcilePreservesWriterAndReaderMembershipWhenMonitorFails(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.LastAppliedMembership.Writer = []string{"db-1"}
	resource.Status.LastAppliedMembership.Reader = []string{"db-2"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{
		{
			InstanceName:         "db-1",
			Endpoint:             "db-1.example",
			Port:                 5432,
			Role:                 v1alpha1.RoleWriter,
			Healthy:              true,
			ConsecutiveSuccesses: 3,
			Reason:               "healthy",
		},
		{
			InstanceName:         "db-2",
			Endpoint:             "db-2.example",
			Port:                 5432,
			Role:                 v1alpha1.RoleReader,
			Healthy:              true,
			ConsecutiveSuccesses: 3,
			Reason:               "healthy",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter},
			{Name: "db-2", Endpoint: "db-2.example", Port: 5432, Role: domain.RoleReader},
		}}},
		Monitor: fakeMonitor{err: errors.New("db-1: monitor configuration failed: password authentication failed for user svc")},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile should not fail on monitor error: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-1" {
		t.Fatalf("writer membership should be preserved: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	if len(updated.Status.LastAppliedMembership.Reader) != 1 || updated.Status.LastAppliedMembership.Reader[0] != "db-2" {
		t.Fatalf("reader membership should be preserved: %#v", updated.Status.LastAppliedMembership.Reader)
	}
	if !hasCondition(updated.Status.Conditions, "MonitorSucceeded", metav1.ConditionFalse) {
		t.Fatalf("MonitorSucceeded false condition missing: %#v", updated.Status.Conditions)
	}
}

func TestReconcilePatchesLivePodMembership(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: resource.Namespace, Labels: map[string]string{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   "sample",
		render.LabelInstance:  "db-1",
	}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource, pod).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleReader,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	updated := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "pod-1", Namespace: resource.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Labels[render.LabelReader] != "true" {
		t.Fatalf("reader membership label missing on live pod: %#v", updated.Labels)
	}
	if updated.Labels[render.LabelRole] != string(v1alpha1.RoleReader) {
		t.Fatalf("role label missing on live pod: %#v", updated.Labels)
	}
}

func TestRequestsForManagedPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pod-1",
		Namespace: "default",
		Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   "sample",
			render.LabelInstance:  "db-1",
		},
	}}

	reconciler := &PgBouncerAuroraReconciler{}
	requests := reconciler.requestsForManagedPod(context.Background(), pod)
	if len(requests) != 1 || requests[0].NamespacedName.Name != "sample" || requests[0].NamespacedName.Namespace != "default" {
		t.Fatalf("request mismatch: %#v", requests)
	}

	pod.Labels[render.LabelManagedBy] = "other"
	if got := reconciler.requestsForManagedPod(context.Background(), pod); len(got) != 0 {
		t.Fatalf("unmanaged pod should not enqueue: %#v", got)
	}
}

func TestRequestsForManagedPodUsesClusterNameAnnotation(t *testing.T) {
	clusterName := "cluster.with.dot.and-a-very-long-name-that-exceeds-kubernetes-label-value-limit"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pod-1",
		Namespace: "default",
		Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   render.ClusterLabelValue(clusterName),
			render.LabelInstance:  "db-1",
		},
		Annotations: map[string]string{render.AnnotationClusterName: clusterName},
	}}

	requests := (&PgBouncerAuroraReconciler{}).requestsForManagedPod(context.Background(), pod)
	if len(requests) != 1 || requests[0].NamespacedName.Name != clusterName {
		t.Fatalf("request mismatch: %#v", requests)
	}
}

func TestRequestsForManagedPodHonorsWatchName(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pod-1",
		Namespace: "default",
		Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   "sample",
			render.LabelInstance:  "db-1",
		},
	}}

	if got := (&PgBouncerAuroraReconciler{WatchName: "other"}).requestsForManagedPod(context.Background(), pod); len(got) != 0 {
		t.Fatalf("watch name mismatch should not enqueue: %#v", got)
	}
	if got := (&PgBouncerAuroraReconciler{WatchName: "*"}).requestsForManagedPod(context.Background(), pod); len(got) != 1 {
		t.Fatalf("star watch name should enqueue: %#v", got)
	}
	if got := (&PgBouncerAuroraReconciler{WatchName: "sample"}).requestsForManagedPod(context.Background(), pod); len(got) != 1 {
		t.Fatalf("matching watch name should enqueue: %#v", got)
	}
	if got := (&PgBouncerAuroraReconciler{WatchName: "other,sample"}).requestsForManagedPod(context.Background(), pod); len(got) != 1 {
		t.Fatalf("matching watch names should enqueue: %#v", got)
	}
}

func TestMatchesWatchName(t *testing.T) {
	for _, watchName := range []string{"", "*", " sample ", "other,sample", "other, sample "} {
		if !(&PgBouncerAuroraReconciler{WatchName: watchName}).matchesWatchName("sample") {
			t.Fatalf("watch name %q should match sample", watchName)
		}
	}
	if (&PgBouncerAuroraReconciler{WatchName: "other"}).matchesWatchName("sample") {
		t.Fatalf("different watch name should not match")
	}
}

func TestReconcileHonorsWatchNameBeforeFetchingResource(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).Build()
	discovery := &countingDiscovery{}
	reconciler := &PgBouncerAuroraReconciler{
		Client:    c,
		Scheme:    scheme,
		Discovery: discovery,
		WatchName: "target",
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "other", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile should ignore non-target watch name without error: %v", err)
	}
	if result != (ctrl.Result{}) {
		t.Fatalf("result should be empty for ignored watch name: %#v", result)
	}
	if discovery.calls != 0 {
		t.Fatalf("discovery should not be called for ignored watch name, calls=%d", discovery.calls)
	}
}

func TestReconcileThrottlesSameResourceWithinMinimumInterval(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	discovery := &countingDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
		Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a",
	}}}}
	monitor := &countingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true, ReadyReplicas: 1}}}
	reconciler := &PgBouncerAuroraReconciler{
		Client:               c,
		Scheme:               scheme,
		Discovery:            discovery,
		Monitor:              monitor,
		ReconcileMinInterval: time.Hour,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}}
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	result, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("second reconcile should throttle without error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("throttled reconcile should requeue after remaining interval: %#v", result)
	}
	if discovery.calls != 1 || monitor.calls != 1 {
		t.Fatalf("second reconcile should not run discovery/monitor: discovery=%d monitor=%d", discovery.calls, monitor.calls)
	}
}

func TestGenerationChangedPredicateIgnoresStatusOnlyUpdates(t *testing.T) {
	oldResource := sampleResource()
	oldResource.Generation = 1
	newResource := oldResource.DeepCopy()
	newResource.Status.ObservedGeneration = 1

	p := predicate.GenerationChangedPredicate{}
	if p.Update(event.UpdateEvent{ObjectOld: oldResource, ObjectNew: newResource}) {
		t.Fatalf("status-only update should not pass generation changed predicate")
	}
	newResource.Generation = 2
	if !p.Update(event.UpdateEvent{ObjectOld: oldResource, ObjectNew: newResource}) {
		t.Fatalf("spec generation update should pass generation changed predicate")
	}
}

func TestMaxConcurrentReconcilesDefaultAndOverride(t *testing.T) {
	if got := (&PgBouncerAuroraReconciler{}).maxConcurrentReconciles(); got != 64 {
		t.Fatalf("default max concurrent reconciles mismatch: %d", got)
	}
	if got := (&PgBouncerAuroraReconciler{MaxConcurrentReconciles: 3}).maxConcurrentReconciles(); got != 3 {
		t.Fatalf("overridden max concurrent reconciles mismatch: %d", got)
	}
}

func TestApplyDesiredRollsDeploymentOnAuthFileSecretResourceVersionChange(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            resource.Spec.PgBouncer.AuthFileSecretRef.Name,
			Namespace:       resource.Namespace,
			UID:             types.UID("secret-uid"),
			ResourceVersion: "1",
		},
		Data: map[string][]byte{"userlist.txt": []byte(`"svc" "md5-old"`)},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource, secret).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: c, Scheme: scheme}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter},
		Replicas:            1,
	}}}

	if err := reconciler.applyDesired(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}
	deployment := assertExists[*appsv1.Deployment](t, ctx, c, "sample-db-1")
	firstHash := deployment.Spec.Template.Annotations[render.AnnotationAuthFileHash]
	if firstHash == "" || firstHash != authFileMetadataHash(secret) {
		t.Fatalf("auth file hash mismatch: got=%q want=%q", firstHash, authFileMetadataHash(secret))
	}

	secret.Data["userlist.txt"] = []byte(`"svc" "md5-new"`)
	if err := c.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.applyDesired(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}
	deployment = assertExists[*appsv1.Deployment](t, ctx, c, "sample-db-1")
	if deployment.Spec.Template.Annotations[render.AnnotationAuthFileHash] == firstHash {
		t.Fatalf("auth file hash should change when secret resourceVersion changes: %s", firstHash)
	}
}

func TestDeploymentDbiMetadataDriftedRequiresNonEmptyIdentity(t *testing.T) {
	resource := sampleResource()
	existing := render.InstanceDeployment(render.InstanceRenderInput{
		Owner: resource,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name:          "db-1",
			DbiResourceId: "dbi-old",
		}},
	})
	expected := render.InstanceDeployment(render.InstanceRenderInput{
		Owner: resource,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name:          "db-1",
			DbiResourceId: "dbi-new",
		}},
	})

	if !deploymentDbiMetadataDrifted(existing, expected) {
		t.Fatalf("expected different non-empty DBI identities to drift")
	}

	delete(existing.Labels, render.LabelDbiResourceID)
	if !deploymentDbiMetadataDrifted(existing, expected) {
		t.Fatalf("expected missing DBI label to be backfilled when desired DBI is known")
	}

	existing.Labels[render.LabelDbiResourceID] = "dbi-old"
	expected.Labels[render.LabelDbiResourceID] = ""
	if deploymentDbiMetadataDrifted(existing, expected) {
		t.Fatalf("unknown desired DBI should not be treated as replacement")
	}
}

func TestPodTemplateDriftIgnoresDeprecatedServiceAccountDefaulting(t *testing.T) {
	resource := sampleResource()
	resource.Spec.PgBouncer.ServiceAccountName = "pgbouncer-options"
	expected := render.InstanceDeployment(render.InstanceRenderInput{
		Owner: resource,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name: "db-1",
		}},
	})
	existing := expected.DeepCopy()
	existing.Spec.Template.Spec.DeprecatedServiceAccount = "pgbouncer-options"

	if !podTemplateSemanticallyEqual(existing.Spec.Template, expected.Spec.Template) {
		t.Fatalf("deprecated serviceAccount defaulting should not drift")
	}
}

func TestApplyDesiredStoresDbiIdentityOnDeploymentMetadataOnly(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: c, Scheme: scheme}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{
			Name:          "db-1",
			Endpoint:      "db-1.example",
			Port:          5432,
			Role:          domain.RoleWriter,
			DbiResourceId: "dbi-new",
		},
		Replicas: 1,
	}}}

	if err := reconciler.applyDesired(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}

	deployment := assertExists[*appsv1.Deployment](t, ctx, c, "sample-db-1")
	if deployment.Labels[render.LabelDbiResourceID] != "dbi-new" {
		t.Fatalf("deployment DBI label mismatch: %q", deployment.Labels[render.LabelDbiResourceID])
	}
	if deployment.Annotations[render.AnnotationDbiResourceID] != "dbi-new" {
		t.Fatalf("deployment DBI annotation mismatch: %q", deployment.Annotations[render.AnnotationDbiResourceID])
	}
	if value := deployment.Spec.Template.Labels[render.LabelDbiResourceID]; value != "" {
		t.Fatalf("pod template must not carry DBI label: %q", value)
	}
	if value := deployment.Spec.Template.Annotations[render.AnnotationDbiResourceID]; value != "" {
		t.Fatalf("pod template must not carry DBI annotation: %q", value)
	}
	if value := deployment.Spec.Selector.MatchLabels[render.LabelDbiResourceID]; value != "" {
		t.Fatalf("selector must not carry DBI label: %q", value)
	}
}

func TestApplyDesiredPreservesDbiIdentityWhenDesiredDbiUnknown(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	existing := render.InstanceDeployment(render.InstanceRenderInput{
		Owner: resource,
		Instance: domain.InstancePlan{InstanceObservation: domain.InstanceObservation{
			Name:          "db-1",
			Endpoint:      "db-1.example",
			Port:          5432,
			Role:          domain.RoleWriter,
			DbiResourceId: "dbi-old",
		}},
	})
	if existing.Spec.Template.Annotations == nil {
		existing.Spec.Template.Annotations = map[string]string{}
	}
	existing.Spec.Template.Annotations[render.AnnotationDbiResourceID] = "dbi-old"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource, existing).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: c, Scheme: scheme}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{
			Name:     "db-1",
			Endpoint: "db-1.example",
			Port:     5432,
			Role:     domain.RoleWriter,
		},
		Replicas: 1,
	}}}

	if err := reconciler.applyDesired(ctx, resource, plan); err != nil {
		t.Fatal(err)
	}

	deployment := assertExists[*appsv1.Deployment](t, ctx, c, "sample-db-1")
	if deployment.Labels[render.LabelDbiResourceID] != "dbi-old" {
		t.Fatalf("known deployment DBI label should be preserved while desired DBI is unknown: %q", deployment.Labels[render.LabelDbiResourceID])
	}
	if deployment.Annotations[render.AnnotationDbiResourceID] != "dbi-old" {
		t.Fatalf("known deployment DBI annotation should be preserved while desired DBI is unknown: %q", deployment.Annotations[render.AnnotationDbiResourceID])
	}
	if value := deployment.Spec.Template.Annotations[render.AnnotationDbiResourceID]; value != "" {
		t.Fatalf("legacy pod template DBI annotation should be removed: %q", value)
	}
}

func TestApplyDesiredSkipsAuthSecretLookupWithoutInstances(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	secretGets := 0
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(resource).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					secretGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: c, Scheme: scheme}

	if err := reconciler.applyDesired(ctx, resource, planner.Output{}); err != nil {
		t.Fatal(err)
	}
	if secretGets != 0 {
		t.Fatalf("secret get count = %d", secretGets)
	}
}

func TestApplyDesiredDeletesStaleRoleServices(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Services.Writer.Name = "writer-v2"
	resource.Spec.Services.Reader.Name = "reader-v2"
	staleWriter := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "sample-writer",
		Namespace: resource.Namespace,
		Labels: map[string]string{
			render.LabelManagedBy:   render.ManagedByValue,
			render.LabelCluster:     render.ClusterLabelValue(resource.Name),
			render.LabelServiceRole: string(v1alpha1.RoleWriter),
		},
	}}
	staleReader := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "sample-reader",
		Namespace: resource.Namespace,
		Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   render.ClusterLabelValue(resource.Name),
		},
	}, Spec: corev1.ServiceSpec{Selector: map[string]string{render.LabelReader: "true"}}}
	perInstance := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      "sample-db-1",
		Namespace: resource.Namespace,
		Labels: map[string]string{
			render.LabelManagedBy:   render.ManagedByValue,
			render.LabelCluster:     render.ClusterLabelValue(resource.Name),
			render.LabelInstance:    "db-1",
			render.LabelServiceRole: string(v1alpha1.RoleWriter),
		},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource, staleWriter, staleReader, perInstance).Build()
	reconciler := &PgBouncerAuroraReconciler{Client: c, Scheme: scheme}

	if err := reconciler.applyDesired(ctx, resource, planner.Output{}); err != nil {
		t.Fatal(err)
	}

	assertNotFound[*corev1.Service](t, ctx, c, "sample-writer")
	assertNotFound[*corev1.Service](t, ctx, c, "sample-reader")
	assertExists[*corev1.Service](t, ctx, c, "sample-writer-v2")
	assertExists[*corev1.Service](t, ctx, c, "sample-reader-v2")
	assertExists[*corev1.Service](t, ctx, c, "sample-db-1")
}

func TestReconcileTracksMissingInstanceAndRetainsResources(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-old"}}
	resource.Status.LastAppliedMembership.Reader = []string{"db-old"}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: resource.Namespace, Labels: map[string]string{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   "sample",
		render.LabelInstance:  "db-old",
		render.LabelReader:    "true",
	}}}
	oldObjects := staleInstanceObjects(resource, "db-old")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource, oldPod}, oldObjects...)...).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.MissingInstances) != 1 ||
		updated.Status.MissingInstances[0].InstanceName != "db-old" ||
		updated.Status.MissingInstances[0].MissingCount != 1 {
		t.Fatalf("missing instance status mismatch: %#v", updated.Status.MissingInstances)
	}
	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "old-pod", Namespace: resource.Namespace}, updatedPod); err != nil {
		t.Fatal(err)
	}
	if updatedPod.Labels[render.LabelReader] != "true" {
		t.Fatalf("missing reader label should be retained before threshold: %#v", updatedPod.Labels)
	}
}

func TestReconcilePreservesMissingInstanceBeforeRemoveThreshold(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{InstanceName: "db-old"}}
	resource.Status.LastAppliedMembership.Reader = []string{"db-old"}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: resource.Namespace, Labels: map[string]string{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
		render.LabelInstance:  "db-old",
		render.LabelReader:    "true",
	}}}
	oldObjects := staleInstanceObjects(resource, "db-old")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource, oldPod}, oldObjects...)...).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{
			Trusted: true,
			Instances: []domain.InstanceObservation{{
				Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
			}},
		}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.MissingInstances) != 1 ||
		updated.Status.MissingInstances[0].InstanceName != "db-old" ||
		updated.Status.MissingInstances[0].MissingCount != 1 {
		t.Fatalf("missing instance status should follow removeAfterMissingCount flow: %#v", updated.Status.MissingInstances)
	}
	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "old-pod", Namespace: resource.Namespace}, updatedPod); err != nil {
		t.Fatal(err)
	}
	if updatedPod.Labels[render.LabelReader] != "true" {
		t.Fatalf("missing reader label should be preserved before threshold: %#v", updatedPod.Labels)
	}
}

func TestReconcileDeletesRemovedInstanceAfterRetention(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	resource.Status.MissingInstances = []v1alpha1.MissingInstanceStatus{{
		InstanceName:     "db-old",
		MissingCount:     3,
		FirstMissingTime: &past,
		LastMissingTime:  &past,
	}}
	oldObjects := staleInstanceObjects(resource, "db-old")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource}, oldObjects...)...).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	assertNotFound[*corev1.ConfigMap](t, ctx, client, "sample-db-old")
	assertNotFound[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	assertNotFound[*corev1.Service](t, ctx, client, "sample-db-old")
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.MissingInstances) != 0 {
		t.Fatalf("deleted missing instance should be dropped from status: %#v", updated.Status.MissingInstances)
	}
}

func TestReconcileExcludesDisabledInstanceOverride(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	disabled := false
	resource.Spec.PgBouncer.InstanceOverrides = []v1alpha1.InstanceOverrideSpec{{Name: "db-2", Enabled: &disabled}}
	oldObjects := staleInstanceObjects(resource, "db-2")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource}, oldObjects...)...).
		Build()
	monitor := &recordingMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: true, ReadyReplicas: 1}}}
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter},
			{Name: "db-2", Endpoint: "db-2.example", Port: 5432, Role: domain.RoleReader},
		}}},
		Monitor: monitor,
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if len(monitor.instances) != 1 || monitor.instances[0].Name != "db-1" {
		t.Fatalf("monitor should only receive enabled instances: %#v", monitor.instances)
	}
	assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	assertNotFound[*corev1.ConfigMap](t, ctx, client, "sample-db-2")
	assertNotFound[*appsv1.Deployment](t, ctx, client, "sample-db-2")
	assertNotFound[*corev1.Service](t, ctx, client, "sample-db-2")

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Reader) != 0 {
		t.Fatalf("reader membership should stay empty when all discovered readers are disabled: %#v", updated.Status.LastAppliedMembership.Reader)
	}
	if !hasCondition(updated.Status.Conditions, "ReaderFallbackSuppressed", metav1.ConditionTrue) ||
		conditionReason(updated.Status.Conditions, "ReaderFallbackSuppressed") != "AllDiscoveredReadersDisabledByOverride" {
		t.Fatalf("ReaderFallbackSuppressed condition mismatch: %#v", updated.Status.Conditions)
	}
	foundDisabled := false
	for _, instance := range updated.Status.Instances {
		if instance.InstanceName == "db-2" {
			foundDisabled = true
			if !instance.Disabled || instance.Healthy || instance.DesiredReplicas != 0 || instance.Reason != "disabled by spec.pgbouncer.instanceOverrides" {
				t.Fatalf("disabled instance status mismatch: %#v", instance)
			}
		}
	}
	if !foundDisabled {
		t.Fatalf("disabled instance should remain visible in status: %#v", updated.Status.Instances)
	}
}

func TestReconcileDeletesRemovedInstanceAfterRetentionWithCachedDiscovery(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	now := metav1.Now()
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.LastAppliedTime = &now
	resource.Status.LastDiscoveryTime = &now
	resource.Status.LastMonitorTime = &now
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{{
		InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleWriter,
	}}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleWriter, Healthy: true, ReadyReplicas: 2,
	}}
	resource.Status.LastAppliedMembership.Writer = []string{"db-1"}
	resource.Status.MissingInstances = []v1alpha1.MissingInstanceStatus{{
		InstanceName:     "db-old",
		MissingCount:     3,
		FirstMissingTime: &past,
		LastMissingTime:  &past,
	}}
	oldObjects := staleInstanceObjects(resource, "db-old")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource}, oldObjects...)...).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client:         client,
		APIReader:      client,
		Scheme:         scheme,
		Discovery:      fakeDiscovery{},
		ScheduleEvents: make(chan event.GenericEvent),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	assertNotFound[*corev1.ConfigMap](t, ctx, client, "sample-db-old")
	assertNotFound[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	assertNotFound[*corev1.Service](t, ctx, client, "sample-db-old")
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.MissingInstances) != 0 {
		t.Fatalf("deleted missing instance should be dropped from status: %#v", updated.Status.MissingInstances)
	}
}

func TestReconcileFastPathSwitchesWriterOnFailoverBeforeMonitorRecovery(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	now := metav1.Now()
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.LastAppliedTime = &now
	resource.Status.LastDiscoveryTime = &metav1.Time{Time: now.Time.Add(-time.Hour)}
	resource.Status.LastMonitorTime = &now
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-old", Endpoint: "db-old.example", Port: 5432, Role: v1alpha1.RoleWriter, AvailabilityZone: "ap-northeast-2a"},
		{InstanceName: "db-new", Endpoint: "db-new.example", Port: 5432, Role: v1alpha1.RoleReader, AvailabilityZone: "ap-northeast-2c"},
	}
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{
		{InstanceName: "db-old", Endpoint: "db-old.example", Port: 5432, Role: v1alpha1.RoleWriter, Healthy: true, ReadyReplicas: 1},
		{InstanceName: "db-new", Endpoint: "db-new.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: false, ReadyReplicas: 1, Reason: "previous monitor failure"},
	}
	resource.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}
	newWriterPod := readyManagedPod(resource, "pod-db-new", "db-new")
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource, newWriterPod).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-old", Endpoint: "db-old.example", Port: 5432, Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2a"},
			{Name: "db-new", Endpoint: "db-new.example", Port: 5432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2c"},
		}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{
			"db-old": {Healthy: true, ReadyReplicas: 1, Reason: "healthy"},
			"db-new": {Healthy: false, ReadyReplicas: 1, Reason: "role mismatch: discovery=writer monitor=reader"},
		}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 1 || updated.Status.LastAppliedMembership.Writer[0] != "db-new" {
		t.Fatalf("writer fast path should switch membership to new writer: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: newWriterPod.Name, Namespace: resource.Namespace}, updatedPod); err != nil {
		t.Fatalf("get pod failed: %v", err)
	}
	if updatedPod.Labels[render.LabelWriter] != "true" {
		t.Fatalf("new writer pod should receive writer label: %#v", updatedPod.Labels)
	}
	if !hasCondition(updated.Status.Conditions, "RoleMismatch", metav1.ConditionTrue) {
		t.Fatalf("role mismatch should still be visible: %#v", updated.Status.Conditions)
	}
}

func TestReconcileUsesAPIReaderFreshStatusForScheduledPlan(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	now := metav1.Now()
	stale := sampleResource()
	stale.Status.ObservedGeneration = stale.Generation
	stale.Status.LastAppliedTime = &now
	stale.Status.LastDiscoveryTime = &now
	stale.Status.LastMonitorTime = &now
	stale.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-0", Endpoint: "db-0.example", Port: 5432, Role: v1alpha1.RoleReader, AvailabilityZone: "ap-northeast-2a"},
		{InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleReader, AvailabilityZone: "ap-northeast-2c"},
		{InstanceName: "db-test", Endpoint: "db-test.example", Port: 5432, Role: v1alpha1.RoleReader, AvailabilityZone: "ap-northeast-2a"},
		{InstanceName: "db-2", Endpoint: "db-2.example", Port: 5432, Role: v1alpha1.RoleWriter, AvailabilityZone: "ap-northeast-2c"},
	}
	stale.Status.TopologyHash = hashObject(stale.Status.LastKnownTopology.Instances)
	stale.Status.LastAppliedMembership.Writer = []string{"db-2"}
	stale.Status.LastAppliedMembership.Reader = []string{"db-test"}
	stale.Status.Instances = []v1alpha1.InstanceStatus{
		{InstanceName: "db-0", Endpoint: "db-0.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: false, ReadyReplicas: 2, Reason: "stale monitor failure"},
		{InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: false, ReadyReplicas: 2, Reason: "stale monitor failure"},
		{InstanceName: "db-test", Endpoint: "db-test.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
		{InstanceName: "db-2", Endpoint: "db-2.example", Port: 5432, Role: v1alpha1.RoleWriter, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
	}
	stale.Status.Conditions = []metav1.Condition{{Type: "DiscoveryTrusted", Status: metav1.ConditionTrue}}

	fresh := stale.DeepCopy()
	fresh.Status.Instances = []v1alpha1.InstanceStatus{
		{InstanceName: "db-0", Endpoint: "db-0.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
		{InstanceName: "db-1", Endpoint: "db-1.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
		{InstanceName: "db-test", Endpoint: "db-test.example", Port: 5432, Role: v1alpha1.RoleReader, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
		{InstanceName: "db-2", Endpoint: "db-2.example", Port: 5432, Role: v1alpha1.RoleWriter, Healthy: true, ReadyReplicas: 2, Reason: "healthy"},
	}

	pods := []client.Object{
		readyManagedPod(stale, "pod-db-0", "db-0"),
		readyManagedPod(stale, "pod-db-1", "db-1"),
		readyManagedPod(stale, "pod-db-test", "db-test"),
		readyManagedPod(stale, "pod-db-2", "db-2"),
	}
	pods[2].GetLabels()[render.LabelReader] = "true"
	pods[3].GetLabels()[render.LabelWriter] = "true"
	pgbaGets := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{fresh}, pods...)...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1alpha1.PgBouncerAurora); ok && key.Name == stale.Name && key.Namespace == stale.Namespace {
					pgbaGets++
					if pgbaGets == 1 {
						*objectAsPgBouncerAurora(t, obj) = *stale.DeepCopy()
						return nil
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client:         client,
		APIReader:      client,
		Scheme:         scheme,
		Discovery:      fakeDiscovery{},
		ScheduleEvents: make(chan event.GenericEvent),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	for _, podName := range []string{"pod-db-0", "pod-db-1", "pod-db-test"} {
		pod := &corev1.Pod{}
		if err := client.Get(ctx, types.NamespacedName{Name: podName, Namespace: stale.Namespace}, pod); err != nil {
			t.Fatalf("get %s failed: %v", podName, err)
		}
		if pod.Labels[render.LabelReader] != "true" {
			t.Fatalf("%s should keep/receive reader membership from fresh status: %#v", podName, pod.Labels)
		}
	}
}

func TestReconcileDropsExpiredMissingWriterMembership(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	resource.Spec.Monitor.FailureThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}
	resource.Status.MissingInstances = []v1alpha1.MissingInstanceStatus{{
		InstanceName:     "db-old",
		MissingCount:     3,
		FirstMissingTime: &past,
		LastMissingTime:  &past,
	}}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: resource.Namespace, Labels: map[string]string{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   "sample",
		render.LabelInstance:  "db-old",
		render.LabelWriter:    "true",
	}}}
	oldObjects := staleInstanceObjects(resource, "db-old")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(append([]client.Object{resource, oldPod}, oldObjects...)...).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-new", Endpoint: "db-new.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-new": {Healthy: false, Reason: "pod not ready"}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 0 {
		t.Fatalf("expired missing writer should be dropped from status: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "old-pod", Namespace: resource.Namespace}, updatedPod); err != nil {
		t.Fatal(err)
	}
	if updatedPod.Labels[render.LabelWriter] != "" {
		t.Fatalf("expired missing writer label should be removed: %#v", updatedPod.Labels)
	}
}

func TestReconcileRemovesObservedUnhealthyWriterMembershipAfterFailureThreshold(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.Monitor.FailureThreshold = 1
	resource.Status.LastAppliedMembership.Writer = []string{"db-1"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName: "db-1",
		Endpoint:     "db-1.example",
		Port:         5432,
		Role:         v1alpha1.RoleWriter,
		Healthy:      true,
	}}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: resource.Namespace, Labels: map[string]string{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   "sample",
		render.LabelInstance:  "db-1",
		render.LabelWriter:    "true",
	}}}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).
		WithObjects(resource, oldPod).
		Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{{
			Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter,
		}}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{"db-1": {Healthy: false, Reason: "pod not ready"}}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updated := &v1alpha1.PgBouncerAurora{}
	if err := client.Get(ctx, types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}, updated); err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if len(updated.Status.LastAppliedMembership.Writer) != 0 {
		t.Fatalf("observed unhealthy writer should be dropped from status: %#v", updated.Status.LastAppliedMembership.Writer)
	}
	if updated.Status.ServiceSummary.Writer.Members != 0 {
		t.Fatalf("writer service summary members should be zero: %#v", updated.Status.ServiceSummary.Writer)
	}
	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "old-pod", Namespace: resource.Namespace}, updatedPod); err != nil {
		t.Fatal(err)
	}
	if updatedPod.Labels[render.LabelWriter] != "" {
		t.Fatalf("observed unhealthy writer label should be removed: %#v", updatedPod.Labels)
	}
}

func TestReconcileDefaultsToRestartWritersOnWriterChange(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-new", Endpoint: "db-new.example", Port: 5432, Role: domain.RoleWriter},
			{Name: "db-old", Endpoint: "db-old.example", Port: 5432, Role: domain.RoleReader},
		}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{
			"db-new": {Healthy: true},
			"db-old": {Healthy: true},
		}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	oldDeployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	restartToken := oldDeployment.Spec.Template.Annotations[restartedAtAnnotation]
	if restartToken == "" {
		t.Fatalf("old writer deployment should be restarted: %#v", oldDeployment.Spec.Template.Annotations)
	}
	newDeployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-new")
	newRestartedAt := newDeployment.Spec.Template.Annotations[restartedAtAnnotation]
	if newRestartedAt == "" {
		t.Fatalf("new writer deployment should be restarted: %#v", newDeployment.Spec.Template.Annotations)
	}
	if newRestartedAt != restartToken {
		t.Fatalf("old/new writer deployments should share the same restart token: old=%q new=%q", restartToken, newRestartedAt)
	}

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	oldDeployment = assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	if oldDeployment.Spec.Template.Annotations[restartedAtAnnotation] != restartToken {
		t.Fatalf("restart annotation should be preserved: %#v", oldDeployment.Spec.Template.Annotations)
	}
	newDeployment = assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-new")
	if newDeployment.Spec.Template.Annotations[restartedAtAnnotation] != restartToken {
		t.Fatalf("new writer restart annotation should be preserved: %#v", newDeployment.Spec.Template.Annotations)
	}
}

func TestReconcileKeepsExistingWhenExplicitlyConfigured(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	resource.Spec.TopologyPolicy.WriterChangeConnectionHandling = v1alpha1.WriterChangeKeepExisting
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.PgBouncerAurora{}).WithObjects(resource).Build()
	reconciler := &PgBouncerAuroraReconciler{
		Client: client,
		Scheme: scheme,
		Discovery: fakeDiscovery{result: domain.DiscoveryResult{Trusted: true, Instances: []domain.InstanceObservation{
			{Name: "db-new", Endpoint: "db-new.example", Port: 5432, Role: domain.RoleWriter},
			{Name: "db-old", Endpoint: "db-old.example", Port: 5432, Role: domain.RoleReader},
		}}},
		Monitor: fakeMonitor{health: map[string]domain.HealthStatus{
			"db-new": {Healthy: true},
			"db-old": {Healthy: true},
		}},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resource.Name, Namespace: resource.Namespace}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	oldDeployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-old")
	if got := oldDeployment.Spec.Template.Annotations[restartedAtAnnotation]; got != "" {
		t.Fatalf("old writer deployment should not be restarted: %q", got)
	}
	newDeployment := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-new")
	if got := newDeployment.Spec.Template.Annotations[restartedAtAnnotation]; got != "" {
		t.Fatalf("new writer deployment should not be restarted: %q", got)
	}
}

func TestRestartInstanceDeploymentsRetriesConflictAndSkipsSameToken(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	resource := sampleResource()
	existing := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "sample-db-1", Namespace: "default"}}
	updateCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updateCount++
				if updateCount == 1 {
					return apierrors.NewConflict(
						schema.GroupResource{Group: "apps", Resource: "deployments"},
						obj.GetName(),
						errors.New("deployment conflict"),
					)
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}

	if err := reconciler.restartInstanceDeployments(ctx, resource, map[string]bool{"db-1": true}, "stable-token"); err != nil {
		t.Fatalf("restartInstanceDeployments failed: %v", err)
	}
	if updateCount != 2 {
		t.Fatalf("update count = %d", updateCount)
	}
	updated := assertExists[*appsv1.Deployment](t, ctx, client, "sample-db-1")
	if updated.Spec.Template.Annotations[restartedAtAnnotation] != "stable-token" {
		t.Fatalf("restart annotation mismatch: %#v", updated.Spec.Template.Annotations)
	}

	if err := reconciler.restartInstanceDeployments(ctx, resource, map[string]bool{"db-1": true}, "stable-token"); err != nil {
		t.Fatalf("second restartInstanceDeployments failed: %v", err)
	}
	if updateCount != 2 {
		t.Fatalf("same token should not update again, update count = %d", updateCount)
	}
}

func TestApplyObjectSkipsNoopUpdate(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sample-db-1", Namespace: "default", Labels: map[string]string{"a": "b"}},
		Data:       map[string]string{"pgbouncer.ini": "same"},
	}
	updateCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updateCount++
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}

	if err := reconciler.applyObject(ctx, existing.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if updateCount != 0 {
		t.Fatalf("noop update count = %d", updateCount)
	}

	changed := existing.DeepCopy()
	changed.Data["pgbouncer.ini"] = "changed"
	if err := reconciler.applyObject(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if updateCount != 1 {
		t.Fatalf("changed update count = %d", updateCount)
	}
}

func TestApplyObjectRetriesConflict(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sample-db-1", Namespace: "default", Labels: map[string]string{"a": "b"}},
		Data:       map[string]string{"pgbouncer.ini": "old"},
	}
	updateCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updateCount++
				if updateCount == 1 {
					return apierrors.NewConflict(
						schema.GroupResource{Group: "", Resource: "configmaps"},
						obj.GetName(),
						errors.New("update conflict"),
					)
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	desired := existing.DeepCopy()
	desired.Data["pgbouncer.ini"] = "new"

	if err := reconciler.applyObject(ctx, desired); err != nil {
		t.Fatalf("applyObject failed: %v", err)
	}
	if updateCount != 2 {
		t.Fatalf("update count = %d", updateCount)
	}
	updated := assertExists[*corev1.ConfigMap](t, ctx, client, "sample-db-1")
	if updated.Data["pgbouncer.ini"] != "new" {
		t.Fatalf("configmap data mismatch: %#v", updated.Data)
	}
}

func TestApplyObjectRetriesCreateAlreadyExists(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sample-db-1", Namespace: "default"},
		Data:       map[string]string{"pgbouncer.ini": "old"},
	}
	firstGet := true
	createCount := 0
	updateCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok && key.Name == existing.Name && firstGet {
					firstGet = false
					return apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					createCount++
					return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updateCount++
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &PgBouncerAuroraReconciler{Client: client, Scheme: scheme}
	desired := existing.DeepCopy()
	desired.Data["pgbouncer.ini"] = "new"

	if err := reconciler.applyObject(ctx, desired); err != nil {
		t.Fatalf("applyObject failed: %v", err)
	}
	if createCount != 1 || updateCount != 1 {
		t.Fatalf("create/update count = %d/%d", createCount, updateCount)
	}
	actual := assertExists[*corev1.ConfigMap](t, ctx, client, existing.Name)
	if actual.Data["pgbouncer.ini"] != "new" {
		t.Fatalf("configmap data = %#v", actual.Data)
	}
}

func TestCopyDesiredPreservesServiceAllocatedFields(t *testing.T) {
	policy := corev1.IPFamilyPolicySingleStack
	loadBalancerClass := "service.k8s.aws/nlb"
	existing := &corev1.Service{Spec: corev1.ServiceSpec{
		Type:                  corev1.ServiceTypeLoadBalancer,
		ClusterIP:             "10.0.0.10",
		ClusterIPs:            []string{"10.0.0.10"},
		IPFamilies:            []corev1.IPFamily{corev1.IPv4Protocol},
		IPFamilyPolicy:        &policy,
		LoadBalancerClass:     &loadBalancerClass,
		ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
		HealthCheckNodePort:   32000,
		Ports: []corev1.ServicePort{{
			Name:     "pgbouncer",
			Port:     6432,
			Protocol: corev1.ProtocolTCP,
			NodePort: 31000,
		}},
	}}
	desired := &corev1.Service{Spec: corev1.ServiceSpec{
		Type:                  corev1.ServiceTypeLoadBalancer,
		ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
		Ports: []corev1.ServicePort{{
			Name:     "pgbouncer",
			Port:     6432,
			Protocol: corev1.ProtocolTCP,
		}},
	}}
	copyDesired(existing, desired)
	if existing.Spec.ClusterIP != "10.0.0.10" ||
		len(existing.Spec.ClusterIPs) != 1 ||
		len(existing.Spec.IPFamilies) != 1 ||
		existing.Spec.IPFamilyPolicy == nil ||
		existing.Spec.LoadBalancerClass == nil ||
		*existing.Spec.LoadBalancerClass != loadBalancerClass ||
		existing.Spec.HealthCheckNodePort != 32000 ||
		existing.Spec.Ports[0].NodePort != 31000 {
		t.Fatalf("allocated service fields not preserved: %#v", existing.Spec)
	}
}

func TestCopyDesiredDropsNodePortFieldsWhenServiceBecomesClusterIP(t *testing.T) {
	existing := &corev1.Service{Spec: corev1.ServiceSpec{
		Type:                corev1.ServiceTypeLoadBalancer,
		ClusterIP:           "10.0.0.10",
		HealthCheckNodePort: 32000,
		Ports: []corev1.ServicePort{{
			Name:     "pgbouncer",
			Port:     6432,
			Protocol: corev1.ProtocolTCP,
			NodePort: 31000,
		}},
	}}
	desired := &corev1.Service{Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{{
			Name:     "pgbouncer",
			Port:     6432,
			Protocol: corev1.ProtocolTCP,
		}},
	}}
	copyDesired(existing, desired)
	if existing.Spec.HealthCheckNodePort != 0 || existing.Spec.Ports[0].NodePort != 0 {
		t.Fatalf("nodeport fields should be dropped for ClusterIP: %#v", existing.Spec)
	}
}

func sampleResource() *v1alpha1.PgBouncerAurora {
	replicas := int32(2)
	return &v1alpha1.PgBouncerAurora{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "PgBouncerAurora"},
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: v1alpha1.PgBouncerAuroraSpec{
			Discovery: v1alpha1.DiscoverySpec{Port: 5432},
			Monitor:   v1alpha1.MonitorSpec{FailureThreshold: 1, RecoveryThreshold: 1},
			PgBouncer: v1alpha1.PgBouncerSpec{
				Image:    "db/pgbouncer:1.25.2",
				Replicas: &replicas,
				Config: v1alpha1.PgBouncerConfigSpec{
					PgBouncer: map[string]string{"listen_port": "6432", "pool_mode": "transaction"},
					Databases: map[string]map[string]string{"*": {"user": "svc"}},
				},
				AuthFileSecretRef: corev1.LocalObjectReference{Name: "pgbouncer-userlist"},
			},
			Services: v1alpha1.ServicesSpec{
				Writer: v1alpha1.ServiceRoleSpec{Name: "writer", Type: corev1.ServiceTypeClusterIP},
				Reader: v1alpha1.ReaderServiceSpec{
					ServiceRoleSpec: v1alpha1.ServiceRoleSpec{Name: "reader", Type: corev1.ServiceTypeClusterIP},
				},
				PerInstances: v1alpha1.PerInstanceServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			TopologyPolicy: v1alpha1.TopologyPolicySpec{
				ZoneAware: v1alpha1.ZoneAwareSpec{Enabled: boolPtr(true), Enforcement: v1alpha1.ZoneAwarePreferred, TopologyKey: "topology.kubernetes.io/zone"},
			},
		},
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func staleInstanceObjects(resource *v1alpha1.PgBouncerAurora, instanceName string) []client.Object {
	name := render.InstanceResourceName(resource.Name, instanceName)
	return []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resource.Namespace}},
	}
}

func readyManagedPod(resource *v1alpha1.PgBouncerAurora, podName string, instanceName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: resource.Namespace,
			Labels: map[string]string{
				render.LabelManagedBy: render.ManagedByValue,
				render.LabelCluster:   render.ClusterLabelValue(resource.Name),
				render.LabelInstance:  instanceName,
			},
			Annotations: map[string]string{render.AnnotationClusterName: resource.Name},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func assertExists[T client.Object](t *testing.T, ctx context.Context, c client.Client, name string) T {
	t.Helper()
	var object T
	switch any(object).(type) {
	case *corev1.ConfigMap:
		object = any(&corev1.ConfigMap{}).(T)
	case *corev1.Service:
		object = any(&corev1.Service{}).(T)
	case *appsv1.Deployment:
		object = any(&appsv1.Deployment{}).(T)
	default:
		t.Fatalf("unsupported type")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, object); err != nil {
		t.Fatalf("expected %s to exist: %v", name, err)
	}
	return object
}

func assertNotFound[T client.Object](t *testing.T, ctx context.Context, c client.Client, name string) {
	t.Helper()
	var object T
	switch any(object).(type) {
	case *corev1.ConfigMap:
		object = any(&corev1.ConfigMap{}).(T)
	case *corev1.Service:
		object = any(&corev1.Service{}).(T)
	case *appsv1.Deployment:
		object = any(&appsv1.Deployment{}).(T)
	default:
		t.Fatalf("unsupported type")
	}
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, object)
	if err == nil {
		t.Fatalf("expected %s to be deleted", name)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected not found for %s, got %v", name, err)
	}
}

func objectAsPgBouncerAurora(t *testing.T, object client.Object) *v1alpha1.PgBouncerAurora {
	t.Helper()
	typed, ok := object.(*v1alpha1.PgBouncerAurora)
	if !ok {
		t.Fatalf("expected PgBouncerAurora, got %T", object)
	}
	return typed
}

func hasCondition(conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == status {
			return true
		}
	}
	return false
}

func TestZoneAwareConflictConditionDefaultsToWarn(t *testing.T) {
	resource := sampleResource()
	resource.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	resource.Spec.TopologyPolicy.ZoneAware.Enforcement = v1alpha1.ZoneAwareRequired
	resource.Spec.TopologyPolicy.ZoneAware.TopologyKey = "topology.kubernetes.io/zone"
	resource.Spec.PgBouncer.NodeSelector = map[string]string{"topology.kubernetes.io/zone": "ap-northeast-2a"}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
		Healthy:             true,
		ReadyReplicas:       1,
	}}}

	status, reason, message := zoneAwareConflictCondition(resource, plan)
	if status != metav1.ConditionTrue || reason != "ZoneAwareConflictWarn" {
		t.Fatalf("condition mismatch: status=%s reason=%s message=%s", status, reason, message)
	}
	if message == "" || !strings.Contains(message, "db-1") || !strings.Contains(message, "ap-northeast-2c") {
		t.Fatalf("message should explain conflicting instance/zone: %q", message)
	}
}

func TestZoneAwareConflictConditionCanIgnore(t *testing.T) {
	resource := sampleResource()
	resource.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	resource.Spec.TopologyPolicy.ZoneAware.ConflictPolicy = v1alpha1.ZoneAwareConflictIgnore
	resource.Spec.PgBouncer.NodeSelector = map[string]string{"topology.kubernetes.io/zone": "ap-northeast-2a"}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
	}}}

	status, reason, message := zoneAwareConflictCondition(resource, plan)
	if status != metav1.ConditionFalse || reason != "ConflictIgnored" || message != "" {
		t.Fatalf("condition mismatch: status=%s reason=%s message=%s", status, reason, message)
	}
}

func TestApplyZoneAwareConflictPolicyFailFreezesPlan(t *testing.T) {
	resource := sampleResource()
	resource.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	resource.Spec.TopologyPolicy.ZoneAware.ConflictPolicy = v1alpha1.ZoneAwareConflictFail
	resource.Spec.TopologyPolicy.ZoneAware.TopologyKey = "topology.kubernetes.io/zone"
	resource.Spec.PgBouncer.NodeSelector = map[string]string{"topology.kubernetes.io/zone": "ap-northeast-2a"}
	plan := planner.Output{Instances: []domain.InstancePlan{{
		InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
		Healthy:             true,
		ReadyReplicas:       1,
	}}}

	applyZoneAwareConflictPolicy(resource, &plan)

	if !plan.Frozen {
		t.Fatalf("plan should be frozen when conflictPolicy=Fail and zone conflict exists")
	}
	if len(plan.Reasons) == 0 || !strings.Contains(plan.Reasons[0], "zoneAware conflict") {
		t.Fatalf("plan reason should describe zoneAware conflict: %#v", plan.Reasons)
	}
}

func TestConditionsReportReaderFallbackSuppressed(t *testing.T) {
	plan := planner.Output{
		Instances: []domain.InstancePlan{
			{
				InstanceObservation: domain.InstanceObservation{Name: "db-1", Role: v1alpha1.RoleWriter},
				Healthy:             true,
				ReadyReplicas:       1,
			},
			{
				InstanceObservation: domain.InstanceObservation{Name: "db-2", Role: v1alpha1.RoleReader},
				Disabled:            true,
			},
		},
		Membership: domain.ServiceMembership{
			Writer: []string{"db-1"},
		},
		ReaderFallbackSuppressed:       true,
		ReaderFallbackSuppressedReason: "AllDiscoveredReadersDisabledByOverride",
	}

	conditions := conditionsFor(sampleResource(), nil, domain.DiscoveryResult{Trusted: true}, plan, "", metav1.Now())
	if conditionReason(conditions, "ReaderFallback") != "SuppressedByDisabledReaders" {
		t.Fatalf("ReaderFallback reason mismatch: %#v", conditions)
	}
	if !hasCondition(conditions, "ReaderFallbackSuppressed", metav1.ConditionTrue) ||
		conditionReason(conditions, "ReaderFallbackSuppressed") != "AllDiscoveredReadersDisabledByOverride" {
		t.Fatalf("ReaderFallbackSuppressed condition mismatch: %#v", conditions)
	}
	if !hasCondition(conditions, "ReaderReady", metav1.ConditionFalse) ||
		conditionReason(conditions, "ReaderReady") != "ReaderMembersDisabledByOverride" {
		t.Fatalf("ReaderReady condition mismatch: %#v", conditions)
	}
}

func conditionReason(conditions []metav1.Condition, conditionType string) string {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Reason
		}
	}
	return ""
}

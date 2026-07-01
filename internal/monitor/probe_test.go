package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"github.com/case-88/pgbouncer-aurora-operator/internal/postgres"
	"github.com/case-88/pgbouncer-aurora-operator/internal/render"
)

type mapDBFactory struct {
	dbs  map[string]*sql.DB
	seen []postgres.ConnInfo
}

func (f *mapDBFactory) Open(ctx context.Context, info postgres.ConnInfo) (*sql.DB, error) {
	f.seen = append(f.seen, info)
	return f.dbs[info.Host], nil
}

type blockingDBFactory struct {
	dbs     map[string]*sql.DB
	delay   time.Duration
	current int32
	max     int32
}

func (f *blockingDBFactory) Open(ctx context.Context, info postgres.ConnInfo) (*sql.DB, error) {
	current := atomic.AddInt32(&f.current, 1)
	for {
		maxSeen := atomic.LoadInt32(&f.max)
		if current <= maxSeen || atomic.CompareAndSwapInt32(&f.max, maxSeen, current) {
			break
		}
	}
	time.Sleep(f.delay)
	atomic.AddInt32(&f.current, -1)
	return f.dbs[info.Host], nil
}

type contextBlockingDBFactory struct{}

func (f contextBlockingDBFactory) Open(ctx context.Context, info postgres.ConnInfo) (*sql.DB, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestProbeMonitorDirectDBHealthy(t *testing.T) {
	resource, c := monitorTestResource(t)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{"db-1.example": db}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if result["db-1"].ReadyReplicas != 1 {
		t.Fatalf("ready replicas = %d", result["db-1"].ReadyReplicas)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProbeMonitorDefaultsToDirectProbe(t *testing.T) {
	resource, c := monitorTestResource(t)
	directDB, directMock := newMockDB(t)
	defer directDB.Close()
	directMock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{
		"db-1.example": directDB,
	}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if len(factory.seen) != 1 {
		t.Fatalf("probe count = %d", len(factory.seen))
	}
	if err := directMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProbeMonitorUsesDiscoverySSLMode(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Discovery.SSLMode = "verify-full"
	directDB, directMock := newMockDB(t)
	defer directDB.Close()
	directMock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{
		"db-1.example": directDB,
	}}

	result, err := (ProbeMonitor{Client: c, DBFactory: factory}).Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if factory.seen[0].SSLMode != "verify-full" {
		t.Fatalf("discovery SSL mode not used: %#v", factory.seen)
	}
}

func TestProbeMonitorRoleMismatch(t *testing.T) {
	resource, c := monitorTestResource(t)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(true, true))
	monitor := ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{"db-1.example": db}}}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if result["db-1"].Healthy {
		t.Fatalf("expected role mismatch unhealthy")
	}
}

func TestProbeMonitorRedactsPasswordFromDirectDBProbeFailure(t *testing.T) {
	resource, c := monitorTestResource(t)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnError(fmt.Errorf("password pw rejected"))
	monitor := ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{"db-1.example": db}}}

	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	reason := result["db-1"].Reason
	if strings.Contains(reason, "pw") {
		t.Fatalf("password leaked in reason: %q", reason)
	}
	if !strings.Contains(reason, "[redacted]") {
		t.Fatalf("redacted marker missing from reason: %q", reason)
	}
}

func TestRoleProbeRejectsReadOnlyWriter(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, true))

	_, err := roleProbe(context.Background(), db)
	if err == nil {
		t.Fatalf("expected role sanity mismatch")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProbeMonitorUsesInternalMaxConcurrency(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	objects := []client.Object{secret}
	instances := make([]domain.InstanceObservation, 0, 5)
	dbs := map[string]*sql.DB{}
	mocks := make([]sqlmock.Sqlmock, 0, 5)
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("db-%d", i)
		host := fmt.Sprintf("%s.example", name)
		objects = append(objects, readyPod("pod-"+name, name))
		instances = append(instances, domain.InstanceObservation{Name: name, Endpoint: host, Port: 5432, Role: domain.RoleWriter})
		db, mock := newMockDB(t)
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
		dbs[host] = db
		mocks = append(mocks, mock)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"
	factory := &blockingDBFactory{dbs: dbs, delay: 10 * time.Millisecond}

	result, err := (ProbeMonitor{Client: c, DBFactory: factory}).Check(context.Background(), resource, instances)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&factory.max) > int32(defaultMonitorProbeConcurrency) {
		t.Fatalf("max concurrency = %d", factory.max)
	}
	for _, instance := range instances {
		if !result[instance.Name].Healthy {
			t.Fatalf("%s health = %#v", instance.Name, result[instance.Name])
		}
	}
	for _, mock := range mocks {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProbeMonitorUsesProbeLimiter(t *testing.T) {
	resource, c := monitorTestResource(t)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	limiter := &fakeWaitLimiter{}

	result, err := (ProbeMonitor{
		Client:       c,
		DBFactory:    &mapDBFactory{dbs: map[string]*sql.DB{"db-1.example": db}},
		ProbeLimiter: limiter,
	}).Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if limiter.calls != 1 {
		t.Fatalf("limiter calls = %d", limiter.calls)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
}

func TestProbeMonitorReturnsErrorWhenProbeLimiterFails(t *testing.T) {
	resource, c := monitorTestResource(t)
	limiter := &fakeWaitLimiter{err: context.DeadlineExceeded}

	result, err := (ProbeMonitor{
		Client:       c,
		DBFactory:    &mapDBFactory{dbs: map[string]*sql.DB{}},
		ProbeLimiter: limiter,
	}).Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err == nil || !strings.Contains(err.Error(), "probe rate limited or monitor job timed out") {
		t.Fatalf("expected limiter error, got result=%#v err=%v", result, err)
	}
	if limiter.calls != 1 {
		t.Fatalf("limiter calls = %d", limiter.calls)
	}
}

func TestProbeMonitorReturnsErrorWhenJobTimeoutExpires(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Monitor.Timeout.Duration = time.Minute

	result, err := (ProbeMonitor{
		Client:     c,
		DBFactory:  contextBlockingDBFactory{},
		JobTimeout: 10 * time.Millisecond,
	}).Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err == nil || !strings.Contains(err.Error(), "monitor job timed out") {
		t.Fatalf("expected monitor job timeout error, got result=%#v err=%v", result, err)
	}
}

func TestProbeMonitorJobTimeoutDefaultAndOverride(t *testing.T) {
	resource, _ := monitorTestResource(t)
	resource.Spec.Monitor.Timeout.Duration = 3 * time.Second
	if got := (ProbeMonitor{}).jobTimeout(resource, 4); got != 8*time.Second {
		t.Fatalf("default job timeout = %s", got)
	}
	if got := (ProbeMonitor{}).jobTimeout(resource, 20); got != 17*time.Second {
		t.Fatalf("scaled job timeout = %s", got)
	}
	if got := (ProbeMonitor{JobTimeout: time.Second}).jobTimeout(resource, 20); got != time.Second {
		t.Fatalf("override job timeout = %s", got)
	}
}

func TestProbeMonitorUsesJobTimeoutForKubernetesReads(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	pod := readyPod("pod-1", "db-1")
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"
	jobTimeout := time.Minute
	listUsedDeadline := false
	getUsedDeadline := false
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(secret, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				assertContextHasActiveDeadline(t, ctx, jobTimeout)
				listUsedDeadline = true
				return c.List(ctx, list, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					assertContextHasActiveDeadline(t, ctx, jobTimeout)
					getUsedDeadline = true
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))

	result, err := (ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{"db-1.example": db}}, JobTimeout: jobTimeout}).Check(
		context.Background(),
		resource,
		[]domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !listUsedDeadline || !getUsedDeadline {
		t.Fatalf("job timeout deadline not used for Kubernetes reads: list=%t get=%t", listUsedDeadline, getUsedDeadline)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type fakeWaitLimiter struct {
	calls int
	err   error
}

func (f *fakeWaitLimiter) Wait(ctx context.Context) error {
	f.calls++
	return f.err
}

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return db, mock
}

func monitorTestResource(t *testing.T) (*v1alpha1.PgBouncerAurora, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	pod := readyPod("pod-1", "db-1")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, pod).Build()
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"
	resource.Spec.PgBouncer.Config.PgBouncer = map[string]string{"listen_port": "6432"}
	return resource, c
}

func TestProbeMonitorRequiresReadyPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"
	monitor := ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{}}}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if result["db-1"].Healthy || result["db-1"].Reason != "pod not ready" {
		t.Fatalf("health = %#v", result["db-1"])
	}
}

func TestProbeMonitorSkipsCredentialsWhenNoReadyPods(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.AuthSecretRef.Name = "missing-db-auth"
	monitor := ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{}}}

	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if result["db-1"].Reason != "pod not ready" {
		t.Fatalf("health = %#v", result["db-1"])
	}
}

func readyPod(name string, instance string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{
			render.LabelManagedBy: render.ManagedByValue,
			render.LabelCluster:   "sample",
			render.LabelInstance:  instance,
		}, Annotations: map[string]string{render.AnnotationClusterName: "sample"}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func assertContextHasActiveDeadline(t *testing.T, ctx context.Context, maxTimeout time.Duration) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > maxTimeout {
		t.Fatalf("deadline remaining = %s, want within (0, %s]", remaining, maxTimeout)
	}
}

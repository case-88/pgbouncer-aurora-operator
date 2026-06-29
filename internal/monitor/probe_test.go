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

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/domain"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/postgres"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/render"
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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

func TestProbeMonitorDefaultsToDirectAndPathProbe(t *testing.T) {
	resource, c := monitorTestResource(t)
	directDB, directMock := newMockDB(t)
	defer directDB.Close()
	pathDB, pathMock := newMockDB(t)
	defer pathDB.Close()
	directMock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	pathMock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{
		"db-1.example":            directDB,
		"sample-db-1.default.svc": pathDB,
	}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if len(factory.seen) != 2 {
		t.Fatalf("probe count = %d", len(factory.seen))
	}
	if factory.seen[0].SSLMode != "require" || factory.seen[1].SSLMode != "disable" {
		t.Fatalf("default sslmodes should be direct=require path=disable: %#v", factory.seen)
	}
	if err := directMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := pathMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProbeMonitorAllowsDirectProbeSSLModeOverrideAndKeepsPathProbeNonTLS(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Monitor.DirectDBSSLMode = "verify-full"
	directDB, directMock := newMockDB(t)
	defer directDB.Close()
	pathDB, pathMock := newMockDB(t)
	defer pathDB.Close()
	directMock.ExpectQuery("select pg_is_in_recovery").WillReturnRows(sqlmock.NewRows([]string{"pg_is_in_recovery", "transaction_read_only"}).AddRow(false, false))
	pathMock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{
		"db-1.example":            directDB,
		"sample-db-1.default.svc": pathDB,
	}}

	result, err := (ProbeMonitor{Client: c, DBFactory: factory}).Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if factory.seen[0].SSLMode != "verify-full" || factory.seen[1].SSLMode != "disable" {
		t.Fatalf("sslmode overrides not used: %#v", factory.seen)
	}
}

func TestEnabledProbesDefaultTrueAndExplicitDisable(t *testing.T) {
	direct, path := enabledProbes(nil, nil)
	if !direct || !path {
		t.Fatalf("nil probe options should default true/true, got %t/%t", direct, path)
	}
	direct, path = enabledProbes(boolPtr(false), boolPtr(true))
	if direct || !path {
		t.Fatalf("explicit false/true mismatch: %t/%t", direct, path)
	}
	direct, path = enabledProbes(boolPtr(false), boolPtr(false))
	if direct || path {
		t.Fatalf("explicit false/false should stay disabled: %t/%t", direct, path)
	}
}

func TestProbeMonitorFailsWhenNoProbeEnabled(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Monitor.DirectDBProbe = boolPtr(false)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
	monitor := ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{}}}

	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if result["db-1"].Healthy || result["db-1"].Reason != "no monitor probes enabled" {
		t.Fatalf("health = %#v", result["db-1"])
	}
}

func TestProbeMonitorRoleMismatch(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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

func TestProbeMonitorPathProbeUsesPerInstanceService(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Monitor.DirectDBProbe = boolPtr(false)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(true)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{"sample-db-1.default.svc": db}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if factory.seen[0].Host != "sample-db-1.default.svc" {
		t.Fatalf("host = %s", factory.seen[0].Host)
	}
}

func TestProbeMonitorPathProbeUsesReservedDatabaseAlias(t *testing.T) {
	resource, c := monitorTestResource(t)
	resource.Spec.Discovery.Database = "appdb"
	resource.Spec.Monitor.DirectDBProbe = boolPtr(false)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(true)
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{"sample-db-1.default.svc": db}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: "db-1", Endpoint: "db-1.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result["db-1"].Healthy {
		t.Fatalf("health = %#v", result["db-1"])
	}
	if factory.seen[0].Database != render.PgBouncerProbeDatabaseAlias {
		t.Fatalf("database = %s, want %s", factory.seen[0].Database, render.PgBouncerProbeDatabaseAlias)
	}
}

func TestProbeMonitorPathProbeUsesTruncatedRenderedServiceName(t *testing.T) {
	resource, _ := monitorTestResource(t)
	resource.Name = "sample-pgbouncer-aurora-operator-cluster-name"
	resource.Spec.Monitor.DirectDBProbe = boolPtr(false)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(true)
	instanceName := "example-aurora-postgresql-instance-identifier-with-long-suffix"
	serviceHost := fmt.Sprintf("%s.default.svc", render.InstanceResourceName(resource.Name, instanceName))
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
		"sslmode":  []byte("disable"),
	}}
	pod := readyPod("pod-long", instanceName)
	pod.Labels[render.LabelCluster] = render.ClusterLabelValue(resource.Name)
	pod.Annotations = map[string]string{render.AnnotationClusterName: resource.Name}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, pod).Build()

	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	factory := &mapDBFactory{dbs: map[string]*sql.DB{serviceHost: db}}
	monitor := ProbeMonitor{Client: c, DBFactory: factory}
	result, err := monitor.Check(context.Background(), resource, []domain.InstanceObservation{{Name: instanceName, Endpoint: "db-long.example", Port: 5432, Role: domain.RoleWriter}})
	if err != nil {
		t.Fatal(err)
	}
	if !result[instanceName].Healthy {
		t.Fatalf("health = %#v", result[instanceName])
	}
	if factory.seen[0].Host != serviceHost {
		t.Fatalf("host = %s, want %s", factory.seen[0].Host, serviceHost)
	}
}

func TestProbeMonitorRespectsMaxConcurrency(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
		"sslmode":  []byte("disable"),
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
	resource.Spec.Monitor.MaxConcurrency = 2
	factory := &blockingDBFactory{dbs: dbs, delay: 10 * time.Millisecond}

	result, err := (ProbeMonitor{Client: c, DBFactory: factory}).Check(context.Background(), resource, instances)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&factory.max) > 2 {
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(true)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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
	if got := (ProbeMonitor{}).jobTimeout(); got != 8*time.Second {
		t.Fatalf("default job timeout = %s", got)
	}
	if got := (ProbeMonitor{JobTimeout: time.Second}).jobTimeout(); got != time.Second {
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
	resource.Spec.Monitor.DirectDBProbe = boolPtr(false)
	resource.Spec.Monitor.PgBouncerPathProbe = boolPtr(false)
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

	result, err := (ProbeMonitor{Client: c, DBFactory: &mapDBFactory{dbs: map[string]*sql.DB{}}, JobTimeout: jobTimeout}).Check(
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
	if result["db-1"].Reason != "no monitor probes enabled" {
		t.Fatalf("health = %#v", result["db-1"])
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
		"sslmode":  []byte("require"),
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

func boolPtr(value bool) *bool {
	return &value
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

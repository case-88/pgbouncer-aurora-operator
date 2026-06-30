package statuspage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
)

func TestRefreshBuildsSnapshotFromCRStatus(t *testing.T) {
	scheme := testScheme()
	now := metav1.Now()
	ready := statusResource("ready", "Ready")
	ready.Status.LastDiscoveryTime = &now
	ready.Status.LastMonitorTime = &now
	ready.Status.LastAppliedTime = &now
	ready.Status.LastAppliedMembership = v1alpha1.MembershipStatus{Writer: []string{"ready-w"}, Reader: []string{"ready-r"}}
	ready.Status.ServiceSummary.Writer.Members = 1
	ready.Status.ServiceSummary.Reader.Members = 1
	ready.Status.Instances = []v1alpha1.InstanceStatus{{InstanceName: "ready-w", Role: v1alpha1.RoleWriter, Healthy: true, ReadyReplicas: 1, DesiredReplicas: 1, Reason: "healthy"}}
	ready.Status.Conditions = []metav1.Condition{
		{Type: "Reconciled", Status: metav1.ConditionTrue, Reason: "Applied"},
		{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "Healthy"},
	}
	degraded := statusResource("degraded", "Degraded")
	degraded.Status.ServiceSummary.Writer.Members = 1
	degraded.Status.ServiceSummary.Reader.Members = 1
	degraded.Status.ServiceSummary.Reader.FallbackFromWriter = true
	degraded.Status.ConsecutiveDiscoveryFailures = 2
	degraded.Status.Conditions = []metav1.Condition{
		{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "MonitorFailed"},
	}
	ignored := statusResource("ignored", "Ready")
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ready, degraded, ignored).Build()
	server := NewServer(Options{Reader: client, Namespace: "db-pgbouncer", WatchName: "*", RefreshMinInterval: time.Second})

	if err := server.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	snapshot := server.Snapshot()
	if snapshot.RefreshMinIntervalSeconds != int64(HardRefreshMinInterval.Seconds()) {
		t.Fatalf("refresh min interval should be clamped: %d", snapshot.RefreshMinIntervalSeconds)
	}
	if snapshot.RecentWindowSeconds != int64(DefaultRecentWindow.Seconds()) {
		t.Fatalf("recent window should use default: %d", snapshot.RecentWindowSeconds)
	}
	if snapshot.Summary.Clusters != 3 || snapshot.Summary.Writers != 2 || snapshot.Summary.Readers != 2 {
		t.Fatalf("summary mismatch: %#v", snapshot.Summary)
	}
	if snapshot.Summary.Degraded != 1 || snapshot.Summary.ReaderFallbacks != 1 || snapshot.Summary.DiscoveryFailureStreak != 2 {
		t.Fatalf("summary health mismatch: %#v", snapshot.Summary)
	}
	if snapshot.Resources[0].Name != "degraded" || snapshot.Resources[0].State != "Degraded" {
		t.Fatalf("resources should be sorted with state: %#v", snapshot.Resources)
	}
}

func TestRefreshHonorsWatchName(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(statusResource("target", "Ready"), statusResource("other", "Ready")).
		Build()
	server := NewServer(Options{Reader: client, Namespace: "db-pgbouncer", WatchName: "target,missing", RefreshMinInterval: 30 * time.Second})
	if err := server.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	snapshot := server.Snapshot()
	if len(snapshot.Resources) != 1 || snapshot.Resources[0].Name != "target" {
		t.Fatalf("watch name filter failed: %#v", snapshot.Resources)
	}
}

func TestHandlersServeHTMLAndJSON(t *testing.T) {
	server := NewServer(Options{RefreshMinInterval: 30 * time.Second})
	htmlRecorder := httptest.NewRecorder()
	server.HTMLHandler().ServeHTTP(htmlRecorder, httptest.NewRequest(http.MethodGet, "/status", nil))
	if htmlRecorder.Code != http.StatusOK || !strings.Contains(htmlRecorder.Body.String(), "PgBouncer Aurora Operator Status") {
		t.Fatalf("html response mismatch: code=%d body=%s", htmlRecorder.Code, htmlRecorder.Body.String())
	}

	jsonRecorder := httptest.NewRecorder()
	server.JSONHandler().ServeHTTP(jsonRecorder, httptest.NewRequest(http.MethodGet, "/status.json", nil))
	if jsonRecorder.Code != http.StatusOK {
		t.Fatalf("json response code = %d", jsonRecorder.Code)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(jsonRecorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("json response invalid: %v", err)
	}
	if snapshot.RefreshMinIntervalSeconds != 30 {
		t.Fatalf("snapshot refresh interval = %d", snapshot.RefreshMinIntervalSeconds)
	}
	if snapshot.RecentWindowSeconds != int64(DefaultRecentWindow.Seconds()) {
		t.Fatalf("snapshot recent window = %d", snapshot.RecentWindowSeconds)
	}
}

func TestClampRecentWindow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value time.Duration
		want  time.Duration
	}{
		{name: "default", value: 0, want: DefaultRecentWindow},
		{name: "minimum", value: time.Second, want: MinRecentWindow},
		{name: "preserve", value: 15 * time.Minute, want: 15 * time.Minute},
		{name: "maximum", value: 48 * time.Hour, want: MaxRecentWindow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClampRecentWindow(tc.value); got != tc.want {
				t.Fatalf("ClampRecentWindow(%s) = %s, want %s", tc.value, got, tc.want)
			}
		})
	}
}

func statusResource(name string, state string) *v1alpha1.PgBouncerAurora {
	resource := &v1alpha1.PgBouncerAurora{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db-pgbouncer", Generation: 1},
	}
	resource.Status.ObservedGeneration = 1
	if state == "Degraded" {
		resource.Status.Conditions = []metav1.Condition{{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "Test"}}
	}
	return resource
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	return scheme
}

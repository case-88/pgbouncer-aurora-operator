package controller

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	auroradiscovery "github.com/case-88/pgbouncer-aurora-operator/internal/discovery"
)

func TestRDSMetadataWorkerDedupeClustersAndHonorsWatchName(t *testing.T) {
	scheme := testScheme(t)
	first := metadataWorkerResource("first", "pg-a")
	second := metadataWorkerResource("second", "pg-a")
	third := metadataWorkerResource("third", "pg-b")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, second, third).Build()
	worker := RDSMetadataWorker{
		Client:    c,
		Namespace: first.Namespace,
		WatchName: "first,second",
	}

	got, err := worker.clusterNames(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"pg-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters = %#v, want %#v", got, want)
	}
}

func TestRDSMetadataWorkerRunsUnderLeaderElection(t *testing.T) {
	if !(&RDSMetadataWorker{}).NeedLeaderElection() {
		t.Fatalf("metadata worker should run only on the elected leader")
	}
}

func TestRDSMetadataWorkerUsesPerClusterDeadline(t *testing.T) {
	scheme := testScheme(t)
	resource := metadataWorkerResource("first", "pg-a")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(resource).Build()
	refresher := &deadlineRefresher{}
	worker := RDSMetadataWorker{
		Client:         c,
		Refresher:      refresher,
		Namespace:      resource.Namespace,
		ClusterTimeout: 250 * time.Millisecond,
	}

	worker.refresh(context.Background())

	if len(refresher.deadlines) != 1 || !refresher.deadlines[0] {
		t.Fatalf("refresh should pass a per-cluster deadline: %#v", refresher.deadlines)
	}
}

func metadataWorkerResource(name string, clusterName string) *v1alpha1.PgBouncerAurora {
	resource := sampleResource()
	resource.ObjectMeta = metav1.ObjectMeta{Name: name, Namespace: "default"}
	resource.Spec.Discovery.ClusterName = clusterName
	return resource
}

type deadlineRefresher struct {
	deadlines []bool
}

func (r *deadlineRefresher) RefreshCluster(ctx context.Context, clusterName string) (map[string]auroradiscovery.InstanceMetadata, error) {
	if clusterName == "" {
		return nil, fmt.Errorf("clusterName is empty")
	}
	_, ok := ctx.Deadline()
	r.deadlines = append(r.deadlines, ok)
	return map[string]auroradiscovery.InstanceMetadata{}, nil
}

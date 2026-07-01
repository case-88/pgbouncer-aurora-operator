package controller

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
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

func metadataWorkerResource(name string, clusterName string) *v1alpha1.PgBouncerAurora {
	resource := sampleResource()
	resource.ObjectMeta = metav1.ObjectMeta{Name: name, Namespace: "default"}
	resource.Spec.Discovery.ClusterName = clusterName
	return resource
}

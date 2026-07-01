package discovery

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/postgres"
)

type fakeDBFactory struct {
	info  postgres.ConnInfo
	infos []postgres.ConnInfo
	err   error
	errs  []error
}

func (f *fakeDBFactory) Open(ctx context.Context, info postgres.ConnInfo) (*sql.DB, error) {
	f.info = info
	f.infos = append(f.infos, info)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return nil, errors.New("stop after credential read")
}

func TestKubernetesRowSourceReadsSecretAndBuildsConnInfo(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := &fakeDBFactory{}
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.ClusterName = "sample"
	resource.Spec.Discovery.DomainName = "example"
	resource.Spec.Discovery.Port = 5432
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"
	resource.Spec.Discovery.Database = "postgres"
	resource.Spec.Discovery.SSLMode = "verify-full"

	_, err := (KubernetesRowSource{Client: client, DBFactory: factory}).Rows(context.Background(), resource)
	if err == nil || !strings.Contains(err.Error(), "stop after credential read") {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(factory.infos) == 0 {
		t.Fatalf("expected open call")
	}
	if factory.infos[0].Host != "sample.cluster-example" || factory.infos[0].Username != "svc" || factory.infos[0].Password != "pw" || factory.infos[0].SSLMode != "verify-full" {
		t.Fatalf("conn info = %#v", factory.infos[0])
	}
}

func TestKubernetesRowSourceFallsBackToReaderEndpoint(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := &fakeDBFactory{errs: []error{errors.New("writer down"), errors.New("reader attempted")}}
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.ClusterEndpoints.Writer.Host = "writer.example"
	resource.Spec.Discovery.ClusterEndpoints.Writer.Port = 32133
	resource.Spec.Discovery.ClusterEndpoints.Reader.Host = "reader.example"
	resource.Spec.Discovery.ClusterEndpoints.Reader.Port = 32134
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"

	_, err := (KubernetesRowSource{Client: client, DBFactory: factory}).Rows(context.Background(), resource)
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(factory.infos) != 2 {
		t.Fatalf("open calls = %#v", factory.infos)
	}
	if factory.infos[0].Host != "writer.example" || factory.infos[0].Port != 32133 {
		t.Fatalf("writer conn info = %#v", factory.infos[0])
	}
	if factory.infos[1].Host != "reader.example" || factory.infos[1].Port != 32134 {
		t.Fatalf("reader conn info = %#v", factory.infos[1])
	}
}

func TestKubernetesRowSourceBuildsClusterEndpointsFromClusterNameAndDomainName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-auth", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	factory := &fakeDBFactory{errs: []error{errors.New("writer down"), errors.New("reader attempted")}}
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.ClusterName = "pg-poc"
	resource.Spec.Discovery.DomainName = "cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com"
	resource.Spec.Discovery.Port = 32133
	resource.Spec.Discovery.AuthSecretRef.Name = "db-auth"

	_, err := (KubernetesRowSource{Client: client, DBFactory: factory}).Rows(context.Background(), resource)
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(factory.infos) != 2 {
		t.Fatalf("open calls = %#v", factory.infos)
	}
	if factory.infos[0].Host != "pg-poc.cluster-cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com" || factory.infos[0].Port != 32133 {
		t.Fatalf("writer conn info = %#v", factory.infos[0])
	}
	if factory.infos[1].Host != "pg-poc.cluster-ro-cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com" || factory.infos[1].Port != 32133 {
		t.Fatalf("reader conn info = %#v", factory.infos[1])
	}
}

func TestKubernetesRowSourceRequiresSecretRef(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"}}
	resource.Spec.Discovery.ClusterName = "sample"
	resource.Spec.Discovery.DomainName = "example"
	_, err := (KubernetesRowSource{}).Rows(context.Background(), resource)
	if err == nil {
		t.Fatalf("expected error")
	}
}

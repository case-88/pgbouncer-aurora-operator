package discovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
)

type fakeRDSClient struct {
	seenClusters  []string
	seenInstances []string
	data          map[string]types.DBInstance
	err           error
}

func (f *fakeRDSClient) DescribeDBClusters(ctx context.Context, params *rds.DescribeDBClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error) {
	if params.DBClusterIdentifier != nil {
		f.seenClusters = append(f.seenClusters, *params.DBClusterIdentifier)
	}
	if f.err != nil {
		return nil, f.err
	}
	return &rds.DescribeDBClustersOutput{DBClusters: []types.DBCluster{{DBClusterIdentifier: stringPtr(rdsTestClusterName)}}}, nil
}

func (f *fakeRDSClient) DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	clusterID := clusterIDFromDescribeInput(params)
	f.seenInstances = append(f.seenInstances, clusterID)
	if f.err != nil {
		return nil, f.err
	}
	instances := make([]types.DBInstance, 0, len(f.data))
	for _, instance := range f.data {
		instances = append(instances, instance)
	}
	return &rds.DescribeDBInstancesOutput{DBInstances: instances}, nil
}

const rdsTestClusterName = "pg-poc"

func TestRDSMetadataResolverResolve(t *testing.T) {
	az := "ap-northeast-2a"
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1")},
	}}
	resolver := RDSMetadataResolver{Client: client}
	metadata, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.seenClusters, []string{rdsTestClusterName}) {
		t.Fatalf("seen clusters = %#v", client.seenClusters)
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName}) {
		t.Fatalf("seen instances = %#v", client.seenInstances)
	}
	got := metadata["db-1"]
	if got.InstanceName != "db-1" || got.AvailabilityZone != az || got.DbiResourceId != "dbi-db-1" {
		t.Fatalf("metadata = %#v", got)
	}
}

func TestRDSMetadataResolverUsesCacheWithinTTL(t *testing.T) {
	az := "ap-northeast-2a"
	addr := "db-1.example"
	addr2 := "db-2.example"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1"), Endpoint: &types.Endpoint{Address: &addr}},
		"db-2": {DBInstanceIdentifier: stringPtr("db-2"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-2"), Endpoint: &types.Endpoint{Address: &addr2}},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}

	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1", "db-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1", "db-2"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName}) {
		t.Fatalf("cache should avoid second API call, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverCacheTTLDefaultAndOverride(t *testing.T) {
	if got := (&RDSMetadataResolver{}).cacheTTL(); got != time.Minute {
		t.Fatalf("default cache ttl mismatch: %v", got)
	}
	if got := (&RDSMetadataResolver{CacheTTL: 30 * time.Second}).cacheTTL(); got != 30*time.Second {
		t.Fatalf("override cache ttl mismatch: %v", got)
	}
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Discovery.MetadataRefreshInterval = metav1.Duration{Duration: 10 * time.Minute}
	if got := (&RDSMetadataResolver{CacheTTL: 30 * time.Second}).cacheTTL(resource); got != 10*time.Minute {
		t.Fatalf("resource metadata refresh interval should win: %v", got)
	}
}

func TestRDSMetadataResolverCachesZoneMetadataWithoutEndpoint(t *testing.T) {
	az := "ap-northeast-2a"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1")},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}

	first, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first["db-1"].AvailabilityZone != az {
		t.Fatalf("first metadata should include AZ: %#v", first["db-1"])
	}

	client.data = map[string]types.DBInstance{}
	second, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if second["db-1"].AvailabilityZone != az {
		t.Fatalf("zone metadata should be served from cache: %#v", second["db-1"])
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName}) {
		t.Fatalf("zone metadata should be cached, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverDoesNotCacheMissingAZ(t *testing.T) {
	az := "ap-northeast-2a"
	addr := "db-1.example"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), Endpoint: &types.Endpoint{Address: &addr}},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}
	resource := rdsSampleResource()

	first, err := resolver.Resolve(context.Background(), resource, []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first["db-1"].AvailabilityZone != "" {
		t.Fatalf("first metadata should miss zone: %#v", first["db-1"])
	}

	client.data["db-1"] = types.DBInstance{DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1"), Endpoint: &types.Endpoint{Address: &addr}}
	second, err := resolver.Resolve(context.Background(), resource, []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if second["db-1"].AvailabilityZone != az {
		t.Fatalf("missing zone metadata must be re-fetched immediately: %#v", second["db-1"])
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName, rdsTestClusterName}) {
		t.Fatalf("missing zone metadata should not be cached, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverDoesNotCacheMissingDbiResourceId(t *testing.T) {
	az := "ap-northeast-2a"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}
	resource := rdsSampleResource()

	first, err := resolver.Resolve(context.Background(), resource, []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first["db-1"].DbiResourceId != "" {
		t.Fatalf("first metadata should miss dbi resource id: %#v", first["db-1"])
	}

	client.data = map[string]types.DBInstance{}
	second, err := resolver.Resolve(context.Background(), resource, []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("metadata missing dbi resource id must not be cached: %#v", second)
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName, rdsTestClusterName}) {
		t.Fatalf("missing dbi resource id metadata should not be cached, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverBypassesCacheForReappearedMissingInstance(t *testing.T) {
	azA := "ap-northeast-2a"
	azC := "ap-northeast-2c"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &azA, DbiResourceId: stringPtr("dbi-old")},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}

	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"}); err != nil {
		t.Fatal(err)
	}

	resource := rdsSampleResource()
	resource.Status.MissingInstances = []v1alpha1.MissingInstanceStatus{{InstanceName: "db-1"}}
	client.data["db-1"] = types.DBInstance{DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &azC, DbiResourceId: stringPtr("dbi-new")}
	second, err := resolver.Resolve(context.Background(), resource, []string{"db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if second["db-1"].AvailabilityZone != azC || second["db-1"].DbiResourceId != "dbi-new" {
		t.Fatalf("reappeared instance should use fresh metadata: %#v", second["db-1"])
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName, rdsTestClusterName}) {
		t.Fatalf("reappeared instance should bypass cache, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverDoesNotUseStaleCacheOnRefreshError(t *testing.T) {
	az := "ap-northeast-2a"
	addr := "db-1.example"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1"), Endpoint: &types.Endpoint{Address: &addr}},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}

	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	client.err = errors.New("rds unavailable")
	metadata, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"})
	if err == nil {
		t.Fatalf("expected refresh error, got metadata=%#v", metadata)
	}
	if !reflect.DeepEqual(client.seenClusters, []string{rdsTestClusterName, rdsTestClusterName}) {
		t.Fatalf("cluster refresh should be attempted once, seen = %#v", client.seenClusters)
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName}) {
		t.Fatalf("instance refresh should not run after cluster error, seen = %#v", client.seenInstances)
	}
}

func TestRDSMetadataResolverDoesNotUseStaleCacheOnIncompleteRefresh(t *testing.T) {
	az := "ap-northeast-2a"
	addr := "db-1.example"
	now := time.Date(2026, 6, 22, 1, 0, 0, 0, time.UTC)
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1"), Endpoint: &types.Endpoint{Address: &addr}},
	}}
	resolver := RDSMetadataResolver{Client: client, CacheTTL: time.Minute, Now: func() time.Time { return now }}

	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	client.data = map[string]types.DBInstance{}
	metadata, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"})
	if err != nil {
		t.Fatalf("incomplete metadata refresh should not be fatal: %v", err)
	}
	if len(metadata) != 0 {
		t.Fatalf("stale metadata must not be returned after incomplete refresh: %#v", metadata)
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

func TestRDSMetadataResolverBatchLookupAndRateLimit(t *testing.T) {
	az := "ap-northeast-2a"
	addr1 := "db-1.example"
	addr2 := "db-2.example"
	client := &fakeRDSClient{data: map[string]types.DBInstance{
		"db-1": {DBInstanceIdentifier: stringPtr("db-1"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-1"), Endpoint: &types.Endpoint{Address: &addr1}},
		"db-2": {DBInstanceIdentifier: stringPtr("db-2"), DBInstanceStatus: stringPtr("available"), AvailabilityZone: &az, DbiResourceId: stringPtr("dbi-db-2"), Endpoint: &types.Endpoint{Address: &addr2}},
	}}
	limiter := &fakeWaitLimiter{}
	resolver := RDSMetadataResolver{Client: client, RateLimiter: limiter}

	metadata, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-2", "db-1", "db-1"})
	if err != nil {
		t.Fatal(err)
	}
	if limiter.calls != 1 {
		t.Fatalf("limiter calls = %d", limiter.calls)
	}
	if !reflect.DeepEqual(client.seenInstances, []string{rdsTestClusterName}) {
		t.Fatalf("expected one cluster inventory call, seen = %#v", client.seenInstances)
	}
	if metadata["db-1"].AvailabilityZone != az || metadata["db-2"].AvailabilityZone != az {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestRDSMetadataResolverReturnsLimiterError(t *testing.T) {
	limiter := &fakeWaitLimiter{err: errors.New("rate limited")}
	resolver := RDSMetadataResolver{Client: &fakeRDSClient{}, RateLimiter: limiter}
	if _, err := resolver.Resolve(context.Background(), rdsSampleResource(), []string{"db-1"}); err == nil {
		t.Fatal("expected limiter error")
	}
}

func clusterIDFromDescribeInput(params *rds.DescribeDBInstancesInput) string {
	for _, filter := range params.Filters {
		if filter.Name != nil && *filter.Name == "db-cluster-id" && len(filter.Values) == 1 {
			return filter.Values[0]
		}
	}
	panic(fmt.Sprintf("unexpected describe input: %#v", params))
}

func rdsSampleResource() *v1alpha1.PgBouncerAurora {
	resource := sampleResource()
	resource.Spec.Discovery.ClusterName = rdsTestClusterName
	return resource
}

func stringPtr(value string) *string {
	return &value
}

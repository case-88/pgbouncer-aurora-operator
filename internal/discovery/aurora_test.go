package discovery

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
)

type fakeRowsSource struct {
	rows []AuroraReplicaStatusRow
	err  error
}

func (f fakeRowsSource) Rows(ctx context.Context, resource *v1alpha1.PgBouncerAurora) ([]AuroraReplicaStatusRow, error) {
	return f.rows, f.err
}

type fakeMetadataResolver struct {
	metadata    map[string]InstanceMetadata
	err         error
	seen        []string
	hasDeadline bool
}

func (f *fakeMetadataResolver) Resolve(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instanceNames []string) (map[string]InstanceMetadata, error) {
	f.seen = append([]string{}, instanceNames...)
	_, f.hasDeadline = ctx.Deadline()
	return f.metadata, f.err
}

func TestRoleFromSessionID(t *testing.T) {
	if got := RoleFromSessionID("MASTER_SESSION_ID"); got != domain.RoleWriter {
		t.Fatalf("role = %s", got)
	}
	if got := RoleFromSessionID("replica-session"); got != domain.RoleReader {
		t.Fatalf("role = %s", got)
	}
}

func TestBuildResultTrusted(t *testing.T) {
	resource := sampleResource()
	result := BuildResult(resource, sampleRows(), sampleMetadata())
	if !result.Trusted {
		t.Fatalf("expected trusted: %s", result.Reason)
	}
	if len(result.Instances) != 2 {
		t.Fatalf("instances = %#v", result.Instances)
	}
	if result.Instances[0].Name != "db-1" || result.Instances[0].Role != domain.RoleWriter {
		t.Fatalf("first instance mismatch: %#v", result.Instances[0])
	}
	if result.Instances[1].Port != 5432 {
		t.Fatalf("spec discovery port should win, got %d", result.Instances[1].Port)
	}
	if result.Instances[0].DbiResourceId != "dbi-db-1" {
		t.Fatalf("dbi resource id should be propagated: %#v", result.Instances[0])
	}
}

func TestBuildResultRejectsEmptyRows(t *testing.T) {
	result := BuildResult(sampleResource(), nil, nil)
	if result.Trusted || !strings.Contains(result.Reason, "no rows") {
		t.Fatalf("result = %#v", result)
	}
}

func TestBuildResultRejectsDuplicateServerID(t *testing.T) {
	rows := []AuroraReplicaStatusRow{{ServerID: "db-1", SessionID: MasterSessionID}, {ServerID: "db-1", SessionID: "reader"}}
	result := BuildResult(sampleResource(), rows, sampleMetadata())
	if result.Trusted || !strings.Contains(result.Reason, "duplicate") {
		t.Fatalf("result = %#v", result)
	}
}

func TestBuildResultRejectsWriterCount(t *testing.T) {
	rows := []AuroraReplicaStatusRow{{ServerID: "db-1", SessionID: "reader"}, {ServerID: "db-2", SessionID: "reader"}}
	result := BuildResult(sampleResource(), rows, sampleMetadata())
	if result.Trusted || !strings.Contains(result.Reason, "exactly one writer") {
		t.Fatalf("result = %#v", result)
	}
}

func TestBuildResultUsesSpecEndpointAndPreviousTopologyMetadataWhenMetadataIsMissing(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "old-db-1.example", Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a", DbiResourceId: "dbi-db-1"},
		{InstanceName: "db-2", Endpoint: "old-db-2.example", Port: 5433, Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c", DbiResourceId: "dbi-db-2"},
	}
	metadata := sampleMetadata()
	delete(metadata, "db-2")
	result := BuildResult(resource, sampleRows(), metadata)
	if !result.Trusted {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Instances) != 2 {
		t.Fatalf("instances mismatch: %#v", result.Instances)
	}
	for _, instance := range result.Instances {
		if instance.Name != "db-2" {
			continue
		}
		if instance.Endpoint != "db-2.example" || instance.Port != 5432 ||
			instance.AvailabilityZone != "ap-northeast-2c" || instance.DbiResourceId != "dbi-db-2" {
			t.Fatalf("spec endpoint/port should win while previous AZ/DBI is reused: %#v", instance)
		}
		return
	}
	t.Fatalf("db-2 not found: %#v", result.Instances)
}

func TestBuildResultFallsBackToPreviousEndpointAndPortWhenSpecCannotBuildThem(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.DomainName = ""
	resource.Spec.Discovery.Port = 0
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "old-db-1.example", Port: 15432, Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "old-db-2.example", Port: 15433, Role: domain.RoleReader},
	}

	result := BuildResult(resource, sampleRows(), nil)
	if !result.Trusted {
		t.Fatalf("result = %#v", result)
	}
	assertObservation(t, result.Instances, "db-1", domain.RoleWriter, "old-db-1.example", 15432, "")
	assertObservation(t, result.Instances, "db-2", domain.RoleReader, "old-db-2.example", 15433, "")
}

func TestBuildResultBuildsNewReaderEndpointWithMetadataAZOnly(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader},
	}
	rows := append(sampleRows(), AuroraReplicaStatusRow{ServerID: "db-3", SessionID: "reader-session"})
	metadata := sampleMetadata()
	metadata["db-3"] = InstanceMetadata{InstanceName: "db-3", AvailabilityZone: "ap-northeast-2a", DbiResourceId: "dbi-db-3"}

	result := BuildResult(resource, rows, metadata)
	if !result.Trusted {
		t.Fatalf("expected trusted result with new reader: %#v", result)
	}
	if len(result.Instances) != 3 {
		t.Fatalf("new reader should be included using deterministic endpoint: %#v", result.Instances)
	}
	assertObservation(t, result.Instances, "db-3", domain.RoleReader, "db-3.example", 5432, "ap-northeast-2a")
}

func TestBuildResultBuildsNewReaderEndpointFromDomainNameWithoutMetadata(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.DomainName = "cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com"
	resource.Spec.Discovery.Port = 32133
	rows := append(sampleRows(), AuroraReplicaStatusRow{ServerID: "db-3", SessionID: "reader-session"})
	metadata := sampleMetadata()
	delete(metadata, "db-3")

	result := BuildResult(resource, rows, metadata)
	if !result.Trusted {
		t.Fatalf("expected trusted result with generated endpoint: %#v", result)
	}
	var found domain.InstanceObservation
	for _, instance := range result.Instances {
		if instance.Name == "db-3" {
			found = instance
		}
	}
	if found.Endpoint != "db-3.cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com" || found.Port != 32133 {
		t.Fatalf("generated endpoint mismatch: %#v", found)
	}
}

func TestBuildResultIgnoresCreatingStatus(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader},
	}
	rows := append(sampleRows(), AuroraReplicaStatusRow{ServerID: "db-3", SessionID: "reader-session"})
	metadata := sampleMetadata()
	metadata["db-3"] = InstanceMetadata{
		InstanceName:     "db-3",
		AvailabilityZone: "ap-northeast-2a",
		DbiResourceId:    "dbi-db-3",
		Status:           "creating",
	}

	result := BuildResult(resource, rows, metadata)
	if !result.Trusted {
		t.Fatalf("expected trusted result with creating metadata ignored: %#v", result)
	}
	if len(result.Instances) != 3 {
		t.Fatalf("metadata status must not exclude SQL-discovered reader: %#v", result.Instances)
	}
}

func TestBuildResultExcludesDeletingReaderFromSQLDiscoveredInstances(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader},
	}
	metadata := sampleMetadata()
	meta := metadata["db-2"]
	meta.Status = "deleting"
	metadata["db-2"] = meta

	result := BuildResult(resource, sampleRows(), metadata)
	if !result.Trusted {
		t.Fatalf("metadata must not make discovery untrusted: %#v", result)
	}
	if len(result.Instances) != 1 || result.Instances[0].Name != "db-1" {
		t.Fatalf("deleting SQL-discovered reader must be excluded from topology: %#v", result.Instances)
	}
	if len(result.RemovingInstances) != 1 || result.RemovingInstances[0] != "db-2" {
		t.Fatalf("removing instances mismatch: %#v", result.RemovingInstances)
	}
}

func TestBuildResultIgnoresMetadataOnlyInstance(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader},
	}
	rows := []AuroraReplicaStatusRow{{ServerID: "db-1", SessionID: MasterSessionID}}
	metadata := sampleMetadata()

	result := BuildResult(resource, rows, metadata)
	if !result.Trusted {
		t.Fatalf("metadata-only instance should not make discovery untrusted: %#v", result)
	}
	if len(result.Instances) != 1 || result.Instances[0].Name != "db-1" {
		t.Fatalf("metadata-only instance should not be in topology: %#v", result.Instances)
	}
}

func TestBuildResultIgnoresMetadataStatusForWriter(t *testing.T) {
	resource := sampleResource()
	metadata := sampleMetadata()

	result := BuildResult(resource, sampleRows(), metadata)
	if !result.Trusted {
		t.Fatalf("metadata status must not override SQL writer role: %#v", result)
	}
}

func TestBuildResultRejectsNewWriterWhenEndpointCannotBeBuilt(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.DomainName = ""
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
	}
	rows := []AuroraReplicaStatusRow{
		{ServerID: "db-1", SessionID: "reader-session"},
		{ServerID: "db-3", SessionID: MasterSessionID},
	}
	metadata := sampleMetadata()
	metadata["db-3"] = InstanceMetadata{InstanceName: "db-3", AvailabilityZone: "ap-northeast-2a", DbiResourceId: "dbi-db-3"}

	result := BuildResult(resource, rows, metadata)
	if result.Trusted || !strings.Contains(result.Reason, "endpoint missing") {
		t.Fatalf("new writer without deterministic endpoint must not be trusted: %#v", result)
	}
}

func TestBuildResultTrustsMissingAZ(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader},
	}
	metadata := sampleMetadata()
	meta := metadata["db-2"]
	meta.AvailabilityZone = ""
	metadata["db-2"] = meta

	result := BuildResult(resource, sampleRows(), metadata)
	if !result.Trusted {
		t.Fatalf("missing AZ must not break SQL discovery trust: %#v", result)
	}
	assertObservation(t, result.Instances, "db-2", domain.RoleReader, "db-2.example", 5432, "")
}

func TestProviderResolvesUniqueSortedMetadata(t *testing.T) {
	resolver := &fakeMetadataResolver{metadata: sampleMetadata()}
	provider := Provider{Rows: fakeRowsSource{rows: sampleRows()}, Metadata: resolver}
	result, err := provider.Discover(context.Background(), sampleResource())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Trusted {
		t.Fatalf("result = %#v", result)
	}
	if !reflect.DeepEqual(resolver.seen, []string{"db-1", "db-2"}) {
		t.Fatalf("resolved names = %#v", resolver.seen)
	}
}

func TestProviderUsesDiscoveryTimeoutForMetadataResolve(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.Timeout.Duration = 2 * time.Second
	resolver := &fakeMetadataResolver{metadata: sampleMetadata()}
	provider := Provider{Rows: fakeRowsSource{rows: sampleRows()}, Metadata: resolver}

	if _, err := provider.Discover(context.Background(), resource); err != nil {
		t.Fatal(err)
	}
	if !resolver.hasDeadline {
		t.Fatalf("metadata resolver should receive a deadline")
	}
}

func TestProviderContinuesWhenMetadataResolveFails(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.DomainName = "cozyhlufqrb3.ap-northeast-2.rds.amazonaws.com"
	resource.Spec.Discovery.Port = 32133
	resolver := &fakeMetadataResolver{err: errors.New("rds unavailable")}
	provider := Provider{Rows: fakeRowsSource{rows: sampleRows()}, Metadata: resolver}

	result, err := provider.Discover(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Trusted {
		t.Fatalf("metadata failure must not make discovery untrusted: %#v", result)
	}
	if !strings.Contains(result.Reason, "rds metadata refresh failed; using fallback") {
		t.Fatalf("metadata warning missing: %#v", result)
	}
	if !reflect.DeepEqual(resolver.seen, []string{"db-1", "db-2"}) {
		t.Fatalf("resolved names = %#v", resolver.seen)
	}
}

func TestProviderUsesSpecEndpointAndPreviousZoneWhenMetadataResolveFails(t *testing.T) {
	resource := sampleResource()
	resource.Spec.Discovery.DomainName = "fresh-domain.example"
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "last-db-1.example", Port: 15432, Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a"},
		{InstanceName: "db-2", Endpoint: "last-db-2.example", Port: 15433, Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
	}
	provider := Provider{
		Rows:     fakeRowsSource{rows: sampleRows()},
		Metadata: &fakeMetadataResolver{err: errors.New("rds unavailable")},
	}

	result, err := provider.Discover(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Trusted {
		t.Fatalf("metadata failure with previous topology must be trusted: %#v", result)
	}
	assertObservation(t, result.Instances, "db-1", domain.RoleWriter, "db-1.fresh-domain.example", 5432, "ap-northeast-2a")
	assertObservation(t, result.Instances, "db-2", domain.RoleReader, "db-2.fresh-domain.example", 5432, "ap-northeast-2c")
}

func TestProviderMetadataErrorDoesNotInferTrafficExclusion(t *testing.T) {
	resource := sampleResource()
	resource.Status.LastKnownTopology.Instances = []v1alpha1.InstanceTopologyStatus{
		{InstanceName: "db-1", Endpoint: "db-1.example", Role: domain.RoleWriter, AvailabilityZone: "ap-northeast-2a"},
		{InstanceName: "db-2", Endpoint: "db-2.example", Role: domain.RoleReader, AvailabilityZone: "ap-northeast-2c"},
	}
	provider := Provider{
		Rows:     fakeRowsSource{rows: sampleRows()},
		Metadata: &fakeMetadataResolver{err: errors.New("rds unavailable")},
	}

	result, err := provider.Discover(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Trusted {
		t.Fatalf("metadata failure must not imply deletion/exclusion: %#v", result)
	}
	if len(result.Instances) != 2 {
		t.Fatalf("all SQL-discovered instances should remain usable: %#v", result.Instances)
	}
}

func TestProviderReturnsUntrustedOnSourceError(t *testing.T) {
	provider := Provider{Rows: fakeRowsSource{err: errors.New("boom")}}
	result, err := provider.Discover(context.Background(), sampleResource())
	if err != nil {
		t.Fatal(err)
	}
	if result.Trusted || !strings.Contains(result.Reason, "query failed") {
		t.Fatalf("result = %#v", result)
	}
}

func sampleResource() *v1alpha1.PgBouncerAurora {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Discovery.DomainName = "example"
	resource.Spec.Discovery.Port = 5432
	resource.Spec.TopologyPolicy.ZoneAware.Enabled = boolPtr(true)
	return resource
}

func boolPtr(value bool) *bool {
	return &value
}

func assertObservation(t *testing.T, instances []domain.InstanceObservation, name string, role domain.Role, endpoint string, port int32, zone string) {
	t.Helper()
	for _, instance := range instances {
		if instance.Name != name {
			continue
		}
		if instance.Role != role || instance.Endpoint != endpoint || instance.Port != port || instance.AvailabilityZone != zone {
			t.Fatalf("observation %s mismatch: %#v", name, instance)
		}
		return
	}
	t.Fatalf("observation %s not found in %#v", name, instances)
}

func sampleRows() []AuroraReplicaStatusRow {
	return []AuroraReplicaStatusRow{
		{ServerID: "db-2", SessionID: "reader-session"},
		{ServerID: "db-1", SessionID: MasterSessionID},
	}
}

func sampleMetadata() map[string]InstanceMetadata {
	return map[string]InstanceMetadata{
		"db-1": {InstanceName: "db-1", AvailabilityZone: "ap-northeast-2a", DbiResourceId: "dbi-db-1"},
		"db-2": {InstanceName: "db-2", AvailabilityZone: "ap-northeast-2c", DbiResourceId: "dbi-db-2"},
	}
}

package discovery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const MasterSessionID = "MASTER_SESSION_ID"

const (
	auroraWriterEndpointTemplate   = "{clusterName}.cluster-{domainName}"
	auroraReaderEndpointTemplate   = "{clusterName}.cluster-ro-{domainName}"
	auroraInstanceEndpointTemplate = "{instanceName}.{domainName}"
)

type AuroraReplicaStatusRow struct {
	ServerID  string
	SessionID string
}

type InstanceMetadata struct {
	InstanceName     string
	AvailabilityZone string
	DbiResourceId    string
	Status           string
}

type RowSource interface {
	Rows(ctx context.Context, resource *v1alpha1.PgBouncerAurora) ([]AuroraReplicaStatusRow, error)
}

type MetadataResolver interface {
	Resolve(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instanceNames []string) (map[string]InstanceMetadata, error)
}

type Provider struct {
	Rows     RowSource
	Metadata MetadataResolver
}

func (p Provider) Discover(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (domain.DiscoveryResult, error) {
	if p.Rows == nil {
		return domain.DiscoveryResult{Trusted: false, Reason: "aurora row source is not configured"}, nil
	}
	rows, err := p.Rows.Rows(ctx, resource)
	if err != nil {
		return domain.DiscoveryResult{Trusted: false, Reason: fmt.Sprintf("aurora replica status query failed: %v", err)}, nil
	}
	instanceNames := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.ServerID != "" {
			instanceNames = append(instanceNames, row.ServerID)
		}
	}
	metadata := map[string]InstanceMetadata{}
	metadataErr := ""
	if p.Metadata != nil && len(instanceNames) > 0 {
		metadataCtx, cancel := context.WithTimeout(ctx, metadataTimeout(resource))
		defer cancel()
		metadata, err = p.Metadata.Resolve(metadataCtx, resource, uniqueSorted(instanceNames))
		if err != nil {
			metadataErr = err.Error()
			log.FromContext(ctx).Error(err, "rds metadata resolve failed; using aurora replica status only",
				"component", "rdsMetadata",
			)
			metadata = map[string]InstanceMetadata{}
		}
	}
	result := BuildResult(resource, rows, metadata)
	if metadataErr != "" {
		result.Reason = fmt.Sprintf("%s; rds metadata refresh failed; using fallback", result.Reason)
	}
	return result, nil
}

func metadataTimeout(resource *v1alpha1.PgBouncerAurora) time.Duration {
	return discoveryTimeout(resource)
}

func BuildResult(resource *v1alpha1.PgBouncerAurora, rows []AuroraReplicaStatusRow, metadata map[string]InstanceMetadata) domain.DiscoveryResult {
	if len(rows) == 0 {
		return untrusted("aurora_replica_status returned no rows")
	}
	previous := previousTopology(resource)
	seen := map[string]struct{}{}
	instances := make([]domain.InstanceObservation, 0, len(rows))
	writerCount := 0
	for _, row := range rows {
		name := strings.TrimSpace(row.ServerID)
		if name == "" {
			return untrusted("aurora_replica_status row has empty server_id")
		}
		if _, ok := seen[name]; ok {
			return untrusted(fmt.Sprintf("duplicate instanceName %q", name))
		}
		seen[name] = struct{}{}

		meta := metadata[name]
		role := RoleFromSessionID(row.SessionID)
		if role == domain.RoleWriter {
			writerCount++
		}
		endpoint := endpointForInstance(resource, name, previous)
		if endpoint == "" {
			return untrusted(fmt.Sprintf("endpoint missing for instance %q", name))
		}
		zone := availabilityZoneForInstance(name, meta, previous)
		dbiResourceId := dbiResourceIdForInstance(name, meta, previous)
		port := portForInstance(resource, name, previous)
		if port == 0 {
			port = 5432
		}
		instances = append(instances, domain.InstanceObservation{
			Name:             name,
			Endpoint:         endpoint,
			Port:             port,
			Role:             role,
			AvailabilityZone: zone,
			DbiResourceId:    dbiResourceId,
		})
	}
	if writerCount != 1 {
		return untrusted(fmt.Sprintf("expected exactly one writer, got %d", writerCount))
	}
	if len(instances) == 0 {
		return untrusted("no usable instances discovered")
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
	return domain.DiscoveryResult{Trusted: true, Instances: instances, Reason: "aurora discovery succeeded"}
}

func RoleFromSessionID(sessionID string) domain.Role {
	if strings.EqualFold(strings.TrimSpace(sessionID), MasterSessionID) {
		return domain.RoleWriter
	}
	return domain.RoleReader
}

func enabledDefaultTrue(value *bool) bool {
	return value == nil || *value
}

func previousTopology(resource *v1alpha1.PgBouncerAurora) map[string]v1alpha1.InstanceTopologyStatus {
	out := map[string]v1alpha1.InstanceTopologyStatus{}
	if resource == nil {
		return out
	}
	for _, instance := range resource.Status.LastKnownTopology.Instances {
		name := strings.TrimSpace(instance.InstanceName)
		if name != "" {
			out[name] = instance
		}
	}
	return out
}

func endpointForInstance(resource *v1alpha1.PgBouncerAurora, name string, previous map[string]v1alpha1.InstanceTopologyStatus) string {
	if resource != nil && strings.TrimSpace(resource.Spec.Discovery.DomainName) != "" {
		return AuroraInstanceEndpoint(name, resource.Spec.Discovery.DomainName)
	}
	if item, ok := previous[name]; ok {
		return strings.TrimSpace(item.Endpoint)
	}
	return ""
}

func availabilityZoneForInstance(name string, meta InstanceMetadata, previous map[string]v1alpha1.InstanceTopologyStatus) string {
	if strings.TrimSpace(meta.AvailabilityZone) != "" {
		return strings.TrimSpace(meta.AvailabilityZone)
	}
	if item, ok := previous[name]; ok {
		return strings.TrimSpace(item.AvailabilityZone)
	}
	return ""
}

func dbiResourceIdForInstance(name string, meta InstanceMetadata, previous map[string]v1alpha1.InstanceTopologyStatus) string {
	if strings.TrimSpace(meta.DbiResourceId) != "" {
		return strings.TrimSpace(meta.DbiResourceId)
	}
	if item, ok := previous[name]; ok {
		return strings.TrimSpace(item.DbiResourceId)
	}
	return ""
}

func portForInstance(resource *v1alpha1.PgBouncerAurora, name string, previous map[string]v1alpha1.InstanceTopologyStatus) int32 {
	if resource != nil && resource.Spec.Discovery.Port > 0 {
		return resource.Spec.Discovery.Port
	}
	if item, ok := previous[name]; ok && item.Port > 0 {
		return item.Port
	}
	return 0
}

func zoneAwareEnabled(resource *v1alpha1.PgBouncerAurora) bool {
	if resource == nil {
		return false
	}
	return enabledDefaultTrue(resource.Spec.TopologyPolicy.ZoneAware.Enabled)
}

func uniqueSorted(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func untrusted(reason string) domain.DiscoveryResult {
	return domain.DiscoveryResult{Trusted: false, Reason: reason}
}

func AuroraWriterEndpoint(clusterName string, domainName string) string {
	return renderAuroraEndpoint(auroraWriterEndpointTemplate, map[string]string{
		"clusterName": clusterName,
		"domainName":  domainName,
	})
}

func AuroraReaderEndpoint(clusterName string, domainName string) string {
	return renderAuroraEndpoint(auroraReaderEndpointTemplate, map[string]string{
		"clusterName": clusterName,
		"domainName":  domainName,
	})
}

func AuroraInstanceEndpoint(instanceName string, domainName string) string {
	return renderAuroraEndpoint(auroraInstanceEndpointTemplate, map[string]string{
		"instanceName": instanceName,
		"domainName":   domainName,
	})
}

func renderAuroraEndpoint(template string, values map[string]string) string {
	out := template
	for key, value := range values {
		out = strings.ReplaceAll(out, "{"+key+"}", strings.TrimSpace(value))
	}
	return out
}

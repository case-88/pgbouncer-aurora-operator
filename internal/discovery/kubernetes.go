package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/postgres"
)

type KubernetesRowSource struct {
	Client    client.Client
	DBFactory postgres.DBFactory
}

type discoveryEndpoint struct {
	Name string
	Host string
	Port int32
}

func (s KubernetesRowSource) Rows(ctx context.Context, resource *v1alpha1.PgBouncerAurora) ([]AuroraReplicaStatusRow, error) {
	if s.Client == nil {
		return nil, fmt.Errorf("kubernetes client is nil")
	}
	factory := s.DBFactory
	if factory == nil {
		factory = postgres.SQLDBFactory{}
	}
	endpoints := discoveryEndpoints(resource)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("spec.discovery.clusterName/domainName or clusterEndpoints.writer.host is empty")
	}
	if resource.Spec.Discovery.AuthSecretRef.Name == "" {
		return nil, fmt.Errorf("spec.discovery.authSecretRef.name is empty")
	}
	secret := &corev1.Secret{}
	if err := s.Client.Get(ctx, types.NamespacedName{Name: resource.Spec.Discovery.AuthSecretRef.Name, Namespace: resource.Namespace}, secret); err != nil {
		return nil, err
	}
	creds, err := postgres.CredentialsFromSecret(secret)
	if err != nil {
		return nil, err
	}
	var errs []error
	for _, endpoint := range endpoints {
		rows, err := s.rowsFromEndpoint(ctx, factory, resource, creds, endpoint)
		if err == nil {
			return rows, nil
		}
		errs = append(errs, fmt.Errorf("%s endpoint %s:%d failed: %w", endpoint.Name, endpoint.Host, endpoint.Port, err))
	}
	return nil, fmt.Errorf("aurora replica status query failed on all discovery endpoints: %w", errors.Join(errs...))
}

func (s KubernetesRowSource) rowsFromEndpoint(
	ctx context.Context,
	factory postgres.DBFactory,
	resource *v1alpha1.PgBouncerAurora,
	creds postgres.Credentials,
	endpoint discoveryEndpoint,
) ([]AuroraReplicaStatusRow, error) {
	queryCtx, cancel := context.WithTimeout(ctx, discoveryTimeout(resource))
	defer cancel()
	db, err := factory.Open(queryCtx, postgres.ConnInfo{
		Host:     endpoint.Host,
		Port:     endpoint.Port,
		Database: defaultString(resource.Spec.Discovery.Database, "postgres"),
		Username: creds.Username,
		Password: creds.Password,
		SSLMode:  resource.Spec.Discovery.SSLMode,
	})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return RowsFromStandardQueryer(queryCtx, db)
}

func discoveryEndpoints(resource *v1alpha1.PgBouncerAurora) []discoveryEndpoint {
	if resource == nil {
		return nil
	}
	spec := resource.Spec.Discovery
	defaultPort := spec.Port
	if defaultPort == 0 {
		defaultPort = 5432
	}
	var endpoints []discoveryEndpoint
	if spec.ClusterEndpoints.Writer.Host != "" {
		endpoints = append(endpoints, discoveryEndpoint{
			Name: "writer",
			Host: spec.ClusterEndpoints.Writer.Host,
			Port: defaultPortInt32(spec.ClusterEndpoints.Writer.Port, defaultPort),
		})
	} else if spec.ClusterName != "" && spec.DomainName != "" {
		endpoints = append(endpoints, discoveryEndpoint{
			Name: "writer",
			Host: AuroraWriterEndpoint(spec.ClusterName, spec.DomainName),
			Port: defaultPort,
		})
	}
	if spec.ClusterEndpoints.Reader.Host != "" {
		endpoints = append(endpoints, discoveryEndpoint{
			Name: "reader",
			Host: spec.ClusterEndpoints.Reader.Host,
			Port: defaultPortInt32(spec.ClusterEndpoints.Reader.Port, defaultPort),
		})
	} else if spec.ClusterName != "" && spec.DomainName != "" {
		endpoints = append(endpoints, discoveryEndpoint{
			Name: "reader",
			Host: AuroraReaderEndpoint(spec.ClusterName, spec.DomainName),
			Port: defaultPort,
		})
	}
	return endpoints
}

func discoveryTimeout(resource *v1alpha1.PgBouncerAurora) time.Duration {
	if resource != nil && resource.Spec.Discovery.Timeout.Duration > 0 {
		return resource.Spec.Discovery.Timeout.Duration
	}
	return 3 * time.Second
}

func defaultPortInt32(value, fallback int32) int32 {
	if value == 0 {
		return fallback
	}
	return value
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

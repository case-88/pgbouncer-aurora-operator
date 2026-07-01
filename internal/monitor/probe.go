package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
	"github.com/case-88/pgbouncer-aurora-operator/internal/postgres"
	"github.com/case-88/pgbouncer-aurora-operator/internal/render"
)

type ProbeMonitor struct {
	Client       client.Client
	DBFactory    postgres.DBFactory
	WorkersPerCR int
	JobTimeout   time.Duration
}

const defaultWorkersPerCR = 10

func (m ProbeMonitor) Check(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) (map[string]domain.HealthStatus, error) {
	instances = enabledInstances(resource, instances)
	jobCtx, cancel := context.WithTimeout(ctx, m.jobTimeout(resource, len(instances)))
	defer cancel()
	readyByInstance, err := m.readyPodsByInstance(jobCtx, resource)
	if err != nil {
		return nil, err
	}
	factory := m.DBFactory
	if factory == nil {
		factory = postgres.SQLDBFactory{}
	}
	out := make(map[string]domain.HealthStatus, len(instances))
	readyInstances := make([]domain.InstanceObservation, 0, len(instances))
	for _, instance := range instances {
		readyReplicas := int32(readyByInstance[instance.Name])
		if readyByInstance[instance.Name] == 0 {
			out[instance.Name] = domain.HealthStatus{Healthy: false, Reason: "pod not ready", ReadyReplicas: readyReplicas}
			continue
		}
		readyInstances = append(readyInstances, instance)
	}
	if len(readyInstances) == 0 {
		return out, nil
	}
	creds, err := m.credentials(jobCtx, resource)
	if err != nil {
		return nil, err
	}
	limit := m.workersPerCR(len(readyInstances))
	jobs := make(chan domain.InstanceObservation)
	results := make(chan probeResult, len(readyInstances))
	var wg sync.WaitGroup
	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for instance := range jobs {
				status := m.checkInstance(jobCtx, resource, factory, creds, instance)
				if err := jobCtx.Err(); err != nil {
					results <- probeResult{name: instance.Name, err: fmt.Errorf("monitor job canceled: %w", err)}
					return
				}
				status.ReadyReplicas = int32(readyByInstance[instance.Name])
				select {
				case results <- probeResult{name: instance.Name, status: status}:
				case <-jobCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, instance := range readyInstances {
			select {
			case jobs <- instance:
			case <-jobCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	remaining := make(map[string]domain.InstanceObservation, len(readyInstances))
	for _, instance := range readyInstances {
		remaining[instance.Name] = instance
	}
	for len(remaining) > 0 {
		select {
		case result, ok := <-results:
			if !ok {
				if len(remaining) > 0 {
					return nil, fmt.Errorf("monitor job finished before %d probe result(s) were collected", len(remaining))
				}
				return out, nil
			}
			if result.err != nil {
				return nil, result.err
			}
			out[result.name] = result.status
			delete(remaining, result.name)
		case <-jobCtx.Done():
			return nil, fmt.Errorf("monitor job canceled: %w", jobCtx.Err())
		}
	}
	return out, nil
}

func enabledInstances(resource *v1alpha1.PgBouncerAurora, instances []domain.InstanceObservation) []domain.InstanceObservation {
	if resource == nil || len(instances) == 0 {
		return instances
	}
	out := make([]domain.InstanceObservation, 0, len(instances))
	for _, instance := range instances {
		if instanceDisabled(resource.Spec.PgBouncer, instance.Name) {
			continue
		}
		out = append(out, instance)
	}
	return out
}

func instanceDisabled(spec v1alpha1.PgBouncerSpec, instanceName string) bool {
	for _, override := range spec.InstanceOverrides {
		if override.Name == instanceName && override.Enabled != nil && !*override.Enabled {
			return true
		}
	}
	return false
}

type probeResult struct {
	name   string
	status domain.HealthStatus
	err    error
}

func (m ProbeMonitor) jobTimeout(resource *v1alpha1.PgBouncerAurora, instanceCount int) time.Duration {
	if m.JobTimeout > 0 {
		return m.JobTimeout
	}
	timeout := monitorProbeTimeout(resource)
	concurrency := m.workersPerCR(instanceCount)
	batches := 1
	if instanceCount > 0 && concurrency > 0 {
		batches = (instanceCount + concurrency - 1) / concurrency
	}
	return clampDuration(time.Duration(batches)*timeout+2*time.Second, 8*time.Second, 30*time.Second)
}

func (m ProbeMonitor) workersPerCR(readyCount int) int {
	limit := m.WorkersPerCR
	if limit <= 0 {
		limit = defaultWorkersPerCR
	}
	if readyCount > 0 && limit > readyCount {
		return readyCount
	}
	return limit
}

func monitorProbeTimeout(resource *v1alpha1.PgBouncerAurora) time.Duration {
	if resource != nil && resource.Spec.Monitor.Timeout.Duration > 0 {
		return resource.Spec.Monitor.Timeout.Duration
	}
	return 3 * time.Second
}

func clampDuration(value, minValue, maxValue time.Duration) time.Duration {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func (m ProbeMonitor) readyPodsByInstance(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (map[string]int, error) {
	pods := &corev1.PodList{}
	if err := m.Client.List(ctx, pods, client.InNamespace(resource.Namespace), client.MatchingLabels{
		render.LabelManagedBy: render.ManagedByValue,
		render.LabelCluster:   render.ClusterLabelValue(resource.Name),
	}); err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, pod := range pods.Items {
		if isPodReady(&pod) {
			out[pod.Labels[render.LabelInstance]]++
		}
	}
	return out, nil
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (m ProbeMonitor) credentials(ctx context.Context, resource *v1alpha1.PgBouncerAurora) (postgres.Credentials, error) {
	if m.Client == nil {
		return postgres.Credentials{}, fmt.Errorf("kubernetes client is nil")
	}
	secretName := resource.Spec.Discovery.AuthSecretRef.Name
	if secretName == "" {
		return postgres.Credentials{}, fmt.Errorf("spec.discovery.authSecretRef.name is empty")
	}
	secret := &corev1.Secret{}
	if err := m.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: resource.Namespace}, secret); err != nil {
		return postgres.Credentials{}, err
	}
	return postgres.CredentialsFromSecret(secret)
}

func (m ProbeMonitor) checkInstance(ctx context.Context, resource *v1alpha1.PgBouncerAurora, factory postgres.DBFactory, creds postgres.Credentials, instance domain.InstanceObservation) domain.HealthStatus {
	var probeCtx context.Context
	var cancel context.CancelFunc
	probeCtx, cancel = context.WithTimeout(ctx, monitorProbeTimeout(resource))
	defer cancel()

	if err := directProbe(probeCtx, resource, factory, creds, instance); err != nil {
		return domain.HealthStatus{Healthy: false, Reason: "direct db probe failed: " + redactCredentialValue(err.Error(), creds.Password)}
	}
	return domain.HealthStatus{Healthy: true, Reason: "healthy"}
}

func redactCredentialValue(message, secret string) string {
	if secret == "" {
		return message
	}
	return strings.ReplaceAll(message, secret, "[redacted]")
}

func directProbe(ctx context.Context, resource *v1alpha1.PgBouncerAurora, factory postgres.DBFactory, creds postgres.Credentials, instance domain.InstanceObservation) error {
	db, err := factory.Open(ctx, postgres.ConnInfo{
		Host:     instance.Endpoint,
		Port:     defaultPort(instance.Port, resource.Spec.Discovery.Port),
		Database: defaultString(resource.Spec.Discovery.Database, "postgres"),
		Username: creds.Username,
		Password: creds.Password,
		SSLMode:  resource.Spec.Discovery.SSLMode,
	})
	if err != nil {
		return err
	}
	defer db.Close()
	role, err := roleProbe(ctx, db)
	if err != nil {
		return err
	}
	if role != instance.Role {
		return fmt.Errorf("role mismatch: discovery=%s monitor=%s", instance.Role, role)
	}
	return nil
}

func roleProbe(ctx context.Context, db *sql.DB) (domain.Role, error) {
	var inRecovery bool
	var transactionReadOnly bool
	if err := db.QueryRowContext(ctx, "select pg_is_in_recovery(), current_setting('transaction_read_only')::boolean").Scan(&inRecovery, &transactionReadOnly); err != nil {
		return "", err
	}
	if inRecovery {
		if !transactionReadOnly {
			return "", fmt.Errorf("role sanity mismatch: pg_is_in_recovery=true transaction_read_only=false")
		}
		return domain.RoleReader, nil
	}
	if transactionReadOnly {
		return "", fmt.Errorf("role sanity mismatch: pg_is_in_recovery=false transaction_read_only=true")
	}
	return domain.RoleWriter, nil
}

func defaultPort(values ...int32) int32 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 5432
}

func defaultString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

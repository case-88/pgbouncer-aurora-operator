package planner

import (
	"fmt"
	"sort"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/domain"
)

type Input struct {
	Resource         *v1alpha1.PgBouncerAurora
	DiscoveryTrusted bool
	Discovered       []domain.InstanceObservation
	Health           map[string]domain.HealthStatus
	CachedHealth     bool
	MissingInstances []v1alpha1.MissingInstanceStatus
}

type Output struct {
	Instances  []domain.InstancePlan
	Membership domain.ServiceMembership
	Frozen     bool
	Reasons    []string
}

func Plan(input Input) Output {
	out := Output{}
	if input.Resource == nil {
		out.Frozen = true
		out.Reasons = append(out.Reasons, "missing resource")
		return out
	}
	if !input.DiscoveryTrusted {
		out.Frozen = true
		out.Reasons = append(out.Reasons, "discovery untrusted")
		out.Instances = instancePlansFromStatus(input.Resource.Status.Instances)
		out.Membership = domain.ServiceMembership{
			Writer: cloneStrings(input.Resource.Status.LastAppliedMembership.Writer),
			Reader: cloneStrings(input.Resource.Status.LastAppliedMembership.Reader),
		}
		return out
	}

	previous := previousInstanceStatus(input.Resource.Status.Instances)
	for _, observed := range input.Discovered {
		previousStatus, hadPrevious := previous[observed.Name]
		if physicalIdentityChanged(observed.DbiResourceId, previousStatus.DbiResourceId) {
			previousStatus = v1alpha1.InstanceStatus{}
			hadPrevious = false
		}
		plan := domain.InstancePlan{
			InstanceObservation: observed,
			Healthy:             false,
			Reason:              "monitor unknown",
			Replicas:            replicasFor(input.Resource.Spec.PgBouncer, observed.Name),
		}
		if input.CachedHealth {
			plan = applyCachedHealth(plan, previousStatus)
		} else if health, ok := input.Health[observed.Name]; ok {
			plan.Healthy = health.Healthy
			plan.Reason = health.Reason
			plan.ReadyReplicas = health.ReadyReplicas
			if plan.Healthy && plan.Reason == "" {
				plan.Reason = "healthy"
			}
		}
		if !input.CachedHealth {
			plan = applyMonitorThresholds(input.Resource, plan, previousStatus, hadPrevious)
		}
		out.Instances = append(out.Instances, plan)
		if !plan.Healthy {
			continue
		}
		switch observed.Role {
		case domain.RoleWriter:
			out.Membership.Writer = append(out.Membership.Writer, observed.Name)
		case domain.RoleReader:
			out.Membership.Reader = append(out.Membership.Reader, observed.Name)
		}
	}

	sortInstancePlans(out.Instances)
	sort.Strings(out.Membership.Writer)
	sort.Strings(out.Membership.Reader)

	stabilizeWriterMembership(input.Resource, &out, input.MissingInstances)
	preserveMissingReaderMembership(input.Resource, &out, input.MissingInstances)
	stabilizeReaderMembership(input.Resource, &out, input.MissingInstances)
	return out
}

func instancePlansFromStatus(items []v1alpha1.InstanceStatus) []domain.InstancePlan {
	out := make([]domain.InstancePlan, 0, len(items))
	for _, item := range items {
		if item.InstanceName == "" {
			continue
		}
		out = append(out, domain.InstancePlan{
			InstanceObservation: domain.InstanceObservation{
				Name:             item.InstanceName,
				Endpoint:         item.Endpoint,
				Port:             item.Port,
				Role:             item.Role,
				AvailabilityZone: item.AvailabilityZone,
				DbiResourceId:    item.DbiResourceId,
			},
			Healthy:              item.Healthy,
			Reason:               item.Reason,
			Replicas:             item.DesiredReplicas,
			ReadyReplicas:        item.ReadyReplicas,
			ConsecutiveFailures:  item.ConsecutiveFailures,
			ConsecutiveSuccesses: item.ConsecutiveSuccesses,
		})
	}
	sortInstancePlans(out)
	return out
}

func physicalIdentityChanged(current, previous string) bool {
	return current != "" && previous != "" && current != previous
}

func applyCachedHealth(plan domain.InstancePlan, previous v1alpha1.InstanceStatus) domain.InstancePlan {
	if previous.InstanceName == "" {
		return plan
	}
	plan.Healthy = previous.Healthy
	plan.Reason = previous.Reason
	plan.ConsecutiveFailures = previous.ConsecutiveFailures
	plan.ConsecutiveSuccesses = previous.ConsecutiveSuccesses
	plan.ReadyReplicas = previous.ReadyReplicas
	return plan
}

func previousInstanceStatus(items []v1alpha1.InstanceStatus) map[string]v1alpha1.InstanceStatus {
	out := make(map[string]v1alpha1.InstanceStatus, len(items))
	for _, item := range items {
		if item.InstanceName != "" {
			out[item.InstanceName] = item
		}
	}
	return out
}

func applyMonitorThresholds(resource *v1alpha1.PgBouncerAurora, plan domain.InstancePlan, previous v1alpha1.InstanceStatus, hadPrevious bool) domain.InstancePlan {
	failureThreshold := defaultInt32(resource.Spec.Monitor.FailureThreshold, 3)
	recoveryThreshold := defaultInt32(resource.Spec.Monitor.RecoveryThreshold, 2)
	rawHealthy := plan.Healthy
	rawReason := plan.Reason

	if rawHealthy {
		plan.ConsecutiveSuccesses = previous.ConsecutiveSuccesses + 1
		plan.ConsecutiveFailures = 0
		if hadPrevious && !previous.Healthy && plan.ConsecutiveSuccesses < recoveryThreshold {
			plan.Healthy = false
			plan.Reason = fmt.Sprintf("recovering %d/%d: %s", plan.ConsecutiveSuccesses, recoveryThreshold, rawReason)
		}
		return plan
	}

	plan.ConsecutiveFailures = previous.ConsecutiveFailures + 1
	plan.ConsecutiveSuccesses = 0
	if previous.Healthy && plan.ConsecutiveFailures < failureThreshold {
		plan.Healthy = true
		plan.Reason = fmt.Sprintf("preserved until failureThreshold %d/%d: %s", plan.ConsecutiveFailures, failureThreshold, rawReason)
	}
	return plan
}

func stabilizeWriterMembership(resource *v1alpha1.PgBouncerAurora, out *Output, missingInstances []v1alpha1.MissingInstanceStatus) {
	if len(out.Membership.Writer) == 1 {
		return
	}
	if len(out.Membership.Writer) > 1 {
		previous := previousCurrentMembership(resource.Status.LastAppliedMembership.Writer, out.Membership.Writer)
		if len(previous) == 1 {
			out.Membership.Writer = previous
			out.Reasons = append(out.Reasons, "writer membership preserved")
			return
		}
		out.Membership.Writer = nil
		out.Reasons = append(out.Reasons, "writer membership ambiguous")
		return
	}
	previous := previousMissingWriterBeforeRemoveThreshold(resource, missingInstances)
	if len(previous) > 0 {
		out.Membership.Writer = previous
		out.Reasons = append(out.Reasons, "writer membership preserved")
		return
	}
}

func previousMissingWriterBeforeRemoveThreshold(resource *v1alpha1.PgBouncerAurora, missingInstances []v1alpha1.MissingInstanceStatus) []string {
	return previousMissingMembershipBeforeRemoveThreshold(resource.Status.LastAppliedMembership.Writer, missingInstances, defaultInt32(resource.Spec.TopologyPolicy.RemoveAfterMissingCount, 3))
}

func stabilizeReaderMembership(resource *v1alpha1.PgBouncerAurora, out *Output, missingInstances []v1alpha1.MissingInstanceStatus) {
	if len(out.Membership.Reader) > 0 {
		return
	}
	if readerEmptyFallbackEnabled(resource) && len(out.Membership.Writer) > 0 {
		out.Membership.Reader = cloneStrings(out.Membership.Writer)
		out.Reasons = append(out.Reasons, "reader fallback to writer")
		return
	}
	previous := previousMissingReaderBeforeRemoveThreshold(resource, missingInstances)
	if len(previous) > 0 {
		out.Membership.Reader = previous
		out.Reasons = append(out.Reasons, "reader membership preserved")
	}
}

func readerEmptyFallbackEnabled(resource *v1alpha1.PgBouncerAurora) bool {
	return enabledDefaultTrue(resource.Spec.TopologyPolicy.ReaderEmptyFallback.Enabled)
}

func previousMissingReaderBeforeRemoveThreshold(resource *v1alpha1.PgBouncerAurora, missingInstances []v1alpha1.MissingInstanceStatus) []string {
	return previousMissingMembershipBeforeRemoveThreshold(resource.Status.LastAppliedMembership.Reader, missingInstances, defaultInt32(resource.Spec.TopologyPolicy.RemoveAfterMissingCount, 3))
}

func previousMissingMembershipBeforeRemoveThreshold(previous []string, missingInstances []v1alpha1.MissingInstanceStatus, threshold int32) []string {
	if len(previous) == 0 {
		return nil
	}
	activeMissing := map[string]bool{}
	for _, missing := range missingInstances {
		if missing.InstanceName != "" && missing.MissingCount < threshold {
			activeMissing[missing.InstanceName] = true
		}
	}
	out := make([]string, 0, len(previous))
	for _, name := range previous {
		if activeMissing[name] {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func previousCurrentMembership(previous []string, current []string) []string {
	if len(previous) == 0 || len(current) == 0 {
		return nil
	}
	currentSet := stringSet(current)
	out := make([]string, 0, len(previous))
	for _, name := range previous {
		if currentSet[name] {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func preserveMissingReaderMembership(resource *v1alpha1.PgBouncerAurora, out *Output, missingInstances []v1alpha1.MissingInstanceStatus) {
	if len(missingInstances) == 0 || len(resource.Status.LastAppliedMembership.Reader) == 0 {
		return
	}
	previousReader := stringSet(resource.Status.LastAppliedMembership.Reader)
	currentReader := stringSet(out.Membership.Reader)
	threshold := defaultInt32(resource.Spec.TopologyPolicy.RemoveAfterMissingCount, 3)
	for _, missing := range missingInstances {
		if missing.InstanceName == "" || missing.MissingCount >= threshold || !previousReader[missing.InstanceName] || currentReader[missing.InstanceName] {
			continue
		}
		out.Membership.Reader = append(out.Membership.Reader, missing.InstanceName)
		out.Reasons = append(out.Reasons, "missing reader membership preserved")
	}
	sort.Strings(out.Membership.Reader)
}

func stringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func replicasFor(spec v1alpha1.PgBouncerSpec, instanceName string) int32 {
	replicas := int32(1)
	if spec.Replicas != nil && *spec.Replicas > 0 {
		replicas = *spec.Replicas
	}
	for _, override := range spec.InstanceOverrides {
		if override.Name == instanceName && override.Replicas > 0 {
			return override.Replicas
		}
	}
	return replicas
}

func defaultInt32(value int32, fallback int32) int32 {
	if value > 0 {
		return value
	}
	return fallback
}

func enabledDefaultTrue(value *bool) bool {
	return value == nil || *value
}

func sortInstancePlans(items []domain.InstancePlan) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

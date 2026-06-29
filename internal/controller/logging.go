package controller

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/domain"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/planner"
)

func logReasonError(reason string) error {
	reason = strings.TrimSpace(truncateLogValue(reason))
	if reason == "" {
		reason = "unknown"
	}
	return fmt.Errorf("%s", reason)
}

func nextDiscoveryFailureCount(resource *v1alpha1.PgBouncerAurora, discoveryFailed bool) int32 {
	if resource == nil || !discoveryFailed {
		return 0
	}
	return resource.Status.ConsecutiveDiscoveryFailures + 1
}

func conditionStatusIs(conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

func planReasonsMessage(plan planner.Output) string {
	message := strings.TrimSpace(strings.Join(plan.Reasons, "; "))
	if message == "" {
		message = "plan frozen"
	}
	return message
}

type topologyLogDiff struct {
	Added   []string
	Removed []string
	Changed []string
}

func topologyDiffForLog(instances []domain.InstanceObservation, statuses []v1alpha1.InstanceTopologyStatus) topologyLogDiff {
	current := make(map[string]domain.InstanceObservation, len(instances))
	previous := make(map[string]v1alpha1.InstanceTopologyStatus, len(statuses))
	for _, instance := range instances {
		if instance.Name == "" {
			continue
		}
		current[instance.Name] = instance
	}
	for _, status := range statuses {
		if status.InstanceName == "" {
			continue
		}
		previous[status.InstanceName] = status
	}
	diff := topologyLogDiff{}
	for name, instance := range current {
		status, ok := previous[name]
		if !ok {
			diff.Added = append(diff.Added, name)
			continue
		}
		if changed := topologyChangeSummary(status, instance); changed != "" {
			diff.Changed = append(diff.Changed, fmt.Sprintf("%s:%s", name, changed))
		}
	}
	for name := range previous {
		if _, ok := current[name]; !ok {
			diff.Removed = append(diff.Removed, name)
		}
	}
	sort.Strings(diff.Added)
	sort.Strings(diff.Removed)
	sort.Strings(diff.Changed)
	return diff
}

func topologyChangeSummary(status v1alpha1.InstanceTopologyStatus, instance domain.InstanceObservation) string {
	changes := make([]string, 0, 5)
	if status.Role != instance.Role {
		changes = append(changes, fmt.Sprintf("role=%s->%s", status.Role, instance.Role))
	}
	if status.Endpoint != instance.Endpoint {
		changes = append(changes, fmt.Sprintf("endpoint=%s->%s", status.Endpoint, instance.Endpoint))
	}
	if status.Port != instance.Port {
		changes = append(changes, fmt.Sprintf("port=%d->%d", status.Port, instance.Port))
	}
	if status.AvailabilityZone != instance.AvailabilityZone {
		changes = append(changes, fmt.Sprintf("az=%s->%s", status.AvailabilityZone, instance.AvailabilityZone))
	}
	if status.DbiResourceId != instance.DbiResourceId {
		changes = append(changes, fmt.Sprintf("dbiResourceId=%s->%s", status.DbiResourceId, instance.DbiResourceId))
	}
	return strings.Join(changes, ",")
}

type healthLogDiff struct {
	Changed []string
}

func healthDiffForLog(statuses []v1alpha1.InstanceStatus, plans []domain.InstancePlan) healthLogDiff {
	previous := make(map[string]v1alpha1.InstanceStatus, len(statuses))
	current := make(map[string]domain.InstancePlan, len(plans))
	for _, status := range statuses {
		if status.InstanceName == "" {
			continue
		}
		previous[status.InstanceName] = status
	}
	for _, plan := range plans {
		if plan.Name == "" {
			continue
		}
		current[plan.Name] = plan
	}
	diff := healthLogDiff{}
	for name, plan := range current {
		status, ok := previous[name]
		if !ok {
			diff.Changed = append(diff.Changed, fmt.Sprintf("%s:added healthy=%t ready=%d/%d reason=%s", name, plan.Healthy, plan.ReadyReplicas, plan.Replicas, truncateLogValue(plan.Reason)))
			continue
		}
		if changed := healthChangeSummary(status, plan); changed != "" {
			diff.Changed = append(diff.Changed, fmt.Sprintf("%s:%s", name, changed))
		}
	}
	for name := range previous {
		if _, ok := current[name]; !ok {
			diff.Changed = append(diff.Changed, fmt.Sprintf("%s:removed", name))
		}
	}
	sort.Strings(diff.Changed)
	return diff
}

func healthChangeSummary(status v1alpha1.InstanceStatus, plan domain.InstancePlan) string {
	changes := make([]string, 0, 4)
	if status.Healthy != plan.Healthy {
		changes = append(changes, fmt.Sprintf("healthy=%t->%t", status.Healthy, plan.Healthy))
	}
	if status.ReadyReplicas != plan.ReadyReplicas || status.DesiredReplicas != plan.Replicas {
		changes = append(changes, fmt.Sprintf("ready=%d/%d->%d/%d", status.ReadyReplicas, status.DesiredReplicas, plan.ReadyReplicas, plan.Replicas))
	}
	if status.Reason != plan.Reason {
		changes = append(changes, fmt.Sprintf("reason=%s->%s", truncateLogValue(status.Reason), truncateLogValue(plan.Reason)))
	}
	return strings.Join(changes, ",")
}

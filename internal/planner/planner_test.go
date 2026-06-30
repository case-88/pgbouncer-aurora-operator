package planner

import (
	"testing"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
)

func TestPlanFreezesOnUntrustedDiscovery(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Status.LastAppliedMembership.Writer = []string{"old-writer"}
	resource.Status.LastAppliedMembership.Reader = []string{"old-reader"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:         "old-writer",
		Endpoint:             "old-writer.example",
		Port:                 5432,
		Role:                 v1alpha1.RoleWriter,
		AvailabilityZone:     "ap-northeast-2a",
		Healthy:              true,
		DesiredReplicas:      2,
		ReadyReplicas:        2,
		ConsecutiveSuccesses: 4,
		Reason:               "healthy",
	}}

	out := Plan(Input{Resource: resource, DiscoveryTrusted: false})
	if !out.Frozen {
		t.Fatalf("expected frozen plan")
	}
	if got := out.Membership.Writer[0]; got != "old-writer" {
		t.Fatalf("writer membership = %s", got)
	}
	if got := out.Membership.Reader[0]; got != "old-reader" {
		t.Fatalf("reader membership = %s", got)
	}
	if len(out.Instances) != 1 || out.Instances[0].Name != "old-writer" || out.Instances[0].ReadyReplicas != 2 {
		t.Fatalf("frozen plan should preserve previous instance status: %#v", out.Instances)
	}
}

func TestPlanReaderEmptyFallback(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{{
			Name: "db-1",
			Role: domain.RoleWriter,
		}},
		Health: map[string]domain.HealthStatus{"db-1": {Healthy: true}},
	})
	if len(out.Membership.Reader) != 1 || out.Membership.Reader[0] != "db-1" {
		t.Fatalf("reader fallback mismatch: %#v", out.Membership.Reader)
	}
}

func TestPlanReaderFallbackDefaultsToWriter(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{{
			Name: "db-1",
			Role: domain.RoleWriter,
		}},
		Health: map[string]domain.HealthStatus{"db-1": {Healthy: true}},
	})
	if len(out.Membership.Reader) != 1 || out.Membership.Reader[0] != "db-1" {
		t.Fatalf("reader fallback default mismatch: %#v", out.Membership.Reader)
	}
}

func TestPlanReaderEmptyFallbackPolicyOverridesDeprecatedServiceField(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.ReaderEmptyFallback.Enabled = boolPtr(false)

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{{
			Name: "db-1",
			Role: domain.RoleWriter,
		}},
		Health: map[string]domain.HealthStatus{"db-1": {Healthy: true}},
	})
	if len(out.Membership.Reader) != 0 {
		t.Fatalf("topology fallback policy should override deprecated service field: %#v", out.Membership.Reader)
	}
}

func TestPlanReplicaOverride(t *testing.T) {
	defaultReplicas := int32(1)
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.PgBouncer.Replicas = &defaultReplicas
	resource.Spec.PgBouncer.InstanceOverrides = []v1alpha1.InstanceOverrideSpec{{Name: "db-2", Replicas: 3}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{
			{Name: "db-1", Role: domain.RoleWriter},
			{Name: "db-2", Role: domain.RoleReader},
		},
		Health: map[string]domain.HealthStatus{
			"db-1": {Healthy: true},
			"db-2": {Healthy: true},
		},
	})
	if out.Instances[0].Replicas != 1 || out.Instances[1].Replicas != 3 {
		t.Fatalf("replicas = %d/%d", out.Instances[0].Replicas, out.Instances[1].Replicas)
	}
}

func TestPlanDoesNotAddMembersWithoutHealth(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{
			{Name: "db-1", Role: domain.RoleWriter},
			{Name: "db-2", Role: domain.RoleReader},
		},
	})

	if len(out.Membership.Writer) != 0 || len(out.Membership.Reader) != 0 {
		t.Fatalf("unknown health must not become service membership: %#v", out.Membership)
	}
	if out.Instances[0].Healthy || out.Instances[1].Healthy {
		t.Fatalf("unknown health must be unhealthy by default: %#v", out.Instances)
	}
}

func TestPlanDropsStalePreviousWriterWhenNoHealthyWriter(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.FailureThreshold = 1
	resource.Status.LastAppliedMembership.Writer = []string{"old-writer"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "new-writer", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"new-writer": {Healthy: false, Reason: "pod not ready"}},
	})

	if len(out.Membership.Writer) != 0 {
		t.Fatalf("stale writer without missing status should not be preserved: %#v", out.Membership.Writer)
	}
}

func TestPlanDropsObservedUnhealthyPreviousWriterAfterFailureThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.FailureThreshold = 1
	resource.Status.LastAppliedMembership.Writer = []string{"db-1"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName: "db-1",
		Healthy:      true,
	}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-1", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-1": {Healthy: false, Reason: "pod not ready"}},
	})

	if out.Instances[0].Healthy {
		t.Fatalf("writer instance should be unhealthy after failure threshold: %#v", out.Instances[0])
	}
	if len(out.Membership.Writer) != 0 {
		t.Fatalf("observed unhealthy writer should be removed from membership: %#v", out.Membership.Writer)
	}
}

func TestPlanPreservesMissingWriterBeforeRemoveThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-new", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-new": {Healthy: false, Reason: "pod not ready"}},
		MissingInstances: []v1alpha1.MissingInstanceStatus{{
			InstanceName: "db-old",
			MissingCount: 2,
		}},
	})

	if len(out.Membership.Writer) != 1 || out.Membership.Writer[0] != "db-old" {
		t.Fatalf("missing writer should be preserved before threshold: %#v", out.Membership.Writer)
	}
}

func TestPlanDropsMissingWriterAtRemoveThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastAppliedMembership.Writer = []string{"db-old"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-new", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-new": {Healthy: false, Reason: "pod not ready"}},
		MissingInstances: []v1alpha1.MissingInstanceStatus{{
			InstanceName: "db-old",
			MissingCount: 3,
		}},
	})

	if len(out.Membership.Writer) != 0 {
		t.Fatalf("missing writer should be dropped at threshold: %#v", out.Membership.Writer)
	}
}

func TestPlanPreservesPreviousWriterWhenWriterAmbiguous(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Status.LastAppliedMembership.Writer = []string{"p1"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{
			{Name: "p1", Role: domain.RoleWriter},
			{Name: "p2", Role: domain.RoleWriter},
		},
		Health: map[string]domain.HealthStatus{
			"p1": {Healthy: true},
			"p2": {Healthy: true},
		},
	})

	if len(out.Membership.Writer) != 1 || out.Membership.Writer[0] != "p1" {
		t.Fatalf("ambiguous writer should preserve previous: %#v", out.Membership.Writer)
	}
}

func TestPlanDropsStalePreviousWriterWhenWriterAmbiguous(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Status.LastAppliedMembership.Writer = []string{"old-writer"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{
			{Name: "p1", Role: domain.RoleWriter},
			{Name: "p2", Role: domain.RoleWriter},
		},
		Health: map[string]domain.HealthStatus{
			"p1": {Healthy: true},
			"p2": {Healthy: true},
		},
	})

	if len(out.Membership.Writer) != 0 {
		t.Fatalf("stale previous writer must not be preserved during ambiguity: %#v", out.Membership.Writer)
	}
}

func TestPlanDropsStalePreviousReaderWhenNoReaderAndNoFallback(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Status.LastAppliedMembership.Reader = []string{"old-reader"}
	resource.Spec.TopologyPolicy.ReaderEmptyFallback.Enabled = boolPtr(false)

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "writer", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"writer": {Healthy: true}},
	})

	if len(out.Membership.Reader) != 0 {
		t.Fatalf("stale reader without missing status should not be preserved: %#v", out.Membership.Reader)
	}
}

func TestPlanDropsObservedUnhealthyPreviousReaderAfterFailureThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.FailureThreshold = 1
	resource.Status.LastAppliedMembership.Reader = []string{"db-2"}
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName: "db-2",
		Healthy:      true,
	}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-2", Role: domain.RoleReader}},
		Health:           map[string]domain.HealthStatus{"db-2": {Healthy: false, Reason: "pod not ready"}},
	})

	if out.Instances[0].Healthy {
		t.Fatalf("reader instance should be unhealthy after failure threshold: %#v", out.Instances[0])
	}
	if len(out.Membership.Reader) != 0 {
		t.Fatalf("observed unhealthy reader should be removed from membership: %#v", out.Membership.Reader)
	}
}

func TestPlanPreservesMissingReaderBeforeRemoveThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastAppliedMembership.Reader = []string{"db-old"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-new", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-new": {Healthy: true}},
		MissingInstances: []v1alpha1.MissingInstanceStatus{{
			InstanceName: "db-old",
			MissingCount: 2,
		}},
	})

	if len(out.Membership.Reader) != 1 || out.Membership.Reader[0] != "db-old" {
		t.Fatalf("missing reader should be preserved before threshold: %#v", out.Membership.Reader)
	}
}

func TestPlanDropsMissingReaderAtRemoveThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Spec.TopologyPolicy.ReaderEmptyFallback.Enabled = boolPtr(false)
	resource.Status.LastAppliedMembership.Reader = []string{"db-old"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-new", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-new": {Healthy: true}},
		MissingInstances: []v1alpha1.MissingInstanceStatus{{
			InstanceName: "db-old",
			MissingCount: 3,
		}},
	})

	if len(out.Membership.Reader) != 0 {
		t.Fatalf("missing reader should be dropped at threshold: %#v", out.Membership.Reader)
	}
}

func TestPlanFallbacksToWriterAfterMissingReaderThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 1
	resource.Spec.TopologyPolicy.RemoveAfterMissingCount = 3
	resource.Status.LastAppliedMembership.Reader = []string{"db-old"}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-new", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-new": {Healthy: true}},
		MissingInstances: []v1alpha1.MissingInstanceStatus{{
			InstanceName: "db-old",
			MissingCount: 3,
		}},
	})

	if len(out.Membership.Reader) != 1 || out.Membership.Reader[0] != "db-new" {
		t.Fatalf("reader should fallback to writer after threshold: %#v", out.Membership.Reader)
	}
}

func TestPlanKeepsHealthyUntilFailureThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.FailureThreshold = 3
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:        "db-1",
		Healthy:             true,
		ConsecutiveFailures: 1,
	}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-1", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-1": {Healthy: false, Reason: "transient"}},
	})

	if !out.Instances[0].Healthy || out.Instances[0].ConsecutiveFailures != 2 {
		t.Fatalf("threshold should preserve health: %#v", out.Instances[0])
	}
	if len(out.Membership.Writer) != 1 || out.Membership.Writer[0] != "db-1" {
		t.Fatalf("membership should be preserved: %#v", out.Membership.Writer)
	}
}

func TestPlanDoesNotCarryHealthAcrossPhysicalReplacement(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.FailureThreshold = 3
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName:         "db-1",
		Healthy:              true,
		ConsecutiveSuccesses: 4,
		DbiResourceId:        "dbi-old",
	}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered: []domain.InstanceObservation{{
			Name:          "db-1",
			Role:          domain.RoleWriter,
			DbiResourceId: "dbi-new",
		}},
		CachedHealth: true,
	})

	if out.Instances[0].Healthy || out.Instances[0].ConsecutiveSuccesses != 0 || out.Instances[0].Reason != "monitor unknown" {
		t.Fatalf("replacement must not inherit cached health: %#v", out.Instances[0])
	}
	if len(out.Membership.Writer) != 0 {
		t.Fatalf("replacement should wait for fresh monitor result: %#v", out.Membership.Writer)
	}
}

func TestPlanWaitsForRecoveryThreshold(t *testing.T) {
	resource := &v1alpha1.PgBouncerAurora{}
	resource.Spec.Monitor.RecoveryThreshold = 2
	resource.Status.Instances = []v1alpha1.InstanceStatus{{
		InstanceName: "db-1",
		Healthy:      false,
	}}

	out := Plan(Input{
		Resource:         resource,
		DiscoveryTrusted: true,
		Discovered:       []domain.InstanceObservation{{Name: "db-1", Role: domain.RoleWriter}},
		Health:           map[string]domain.HealthStatus{"db-1": {Healthy: true, Reason: "healthy"}},
	})

	if out.Instances[0].Healthy || out.Instances[0].ConsecutiveSuccesses != 1 {
		t.Fatalf("first recovery observation should stay unhealthy: %#v", out.Instances[0])
	}
	if len(out.Membership.Writer) != 0 {
		t.Fatalf("membership should wait for recovery threshold: %#v", out.Membership.Writer)
	}
}

func boolPtr(value bool) *bool {
	return &value
}

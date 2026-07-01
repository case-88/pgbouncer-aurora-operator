package domain

import v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"

type Role = v1alpha1.Role

const (
	RoleWriter = v1alpha1.RoleWriter
	RoleReader = v1alpha1.RoleReader
)

type InstanceObservation struct {
	Name             string
	Endpoint         string
	Port             int32
	Role             Role
	AvailabilityZone string
	DbiResourceId    string
}

type DiscoveryResult struct {
	Trusted   bool
	Instances []InstanceObservation
	Reason    string
}

type HealthStatus struct {
	Healthy       bool
	Reason        string
	ReadyReplicas int32
}

type InstancePlan struct {
	InstanceObservation
	Healthy              bool
	Disabled             bool
	Reason               string
	Replicas             int32
	ReadyReplicas        int32
	ConsecutiveFailures  int32
	ConsecutiveSuccesses int32
}

type ServiceMembership struct {
	Writer []string
	Reader []string
}

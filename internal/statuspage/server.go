package statuspage

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
)

const (
	DefaultRefreshMinInterval = 5 * time.Second
	HardRefreshMinInterval    = 5 * time.Second
	DefaultRecentWindow       = time.Minute
	MinRecentWindow           = time.Minute
	MaxRecentWindow           = 24 * time.Hour
)

type Options struct {
	Reader             client.Reader
	Namespace          string
	WatchName          string
	RefreshMinInterval time.Duration
	RecentWindow       time.Duration
	Log                logr.Logger
}

type Server struct {
	reader             client.Reader
	namespace          string
	watchName          string
	refreshMinInterval time.Duration
	recentWindow       time.Duration
	log                logr.Logger

	mu       sync.RWMutex
	snapshot Snapshot
}

type Snapshot struct {
	GeneratedAt               time.Time        `json:"generatedAt"`
	Namespace                 string           `json:"namespace"`
	WatchName                 string           `json:"watchName"`
	RefreshMinIntervalSeconds int64            `json:"refreshMinIntervalSeconds"`
	RecentWindowSeconds       int64            `json:"recentWindowSeconds"`
	Summary                   Summary          `json:"summary"`
	Resources                 []ResourceStatus `json:"resources"`
	Error                     string           `json:"error,omitempty"`
}

type Summary struct {
	Clusters               int `json:"clusters"`
	Writers                int `json:"writers"`
	Readers                int `json:"readers"`
	Degraded               int `json:"degraded"`
	Frozen                 int `json:"frozen"`
	ReaderFallbacks        int `json:"readerFallbacks"`
	DiscoveryFailureStreak int `json:"discoveryFailureStreak"`
}

type ResourceStatus struct {
	Namespace                    string                           `json:"namespace"`
	Name                         string                           `json:"name"`
	State                        string                           `json:"state"`
	Generation                   int64                            `json:"generation"`
	ObservedGeneration           int64                            `json:"observedGeneration"`
	TopologyHash                 string                           `json:"topologyHash,omitempty"`
	MembershipHash               string                           `json:"membershipHash,omitempty"`
	ConsecutiveDiscoveryFailures int32                            `json:"consecutiveDiscoveryFailures,omitempty"`
	LastDiscoveryTime            *metav1.Time                     `json:"lastDiscoveryTime,omitempty"`
	LastMonitorTime              *metav1.Time                     `json:"lastMonitorTime,omitempty"`
	LastAppliedTime              *metav1.Time                     `json:"lastAppliedTime,omitempty"`
	LastKnownTopology            v1alpha1.TopologyStatus          `json:"lastKnownTopology,omitempty"`
	LastAppliedMembership        v1alpha1.MembershipStatus        `json:"lastAppliedMembership,omitempty"`
	ServiceSummary               v1alpha1.ServiceSummaryStatus    `json:"serviceSummary,omitempty"`
	MissingInstances             []v1alpha1.MissingInstanceStatus `json:"missingInstances,omitempty"`
	Instances                    []v1alpha1.InstanceStatus        `json:"instances,omitempty"`
	Conditions                   []metav1.Condition               `json:"conditions,omitempty"`
}

func NewServer(options Options) *Server {
	refreshMinInterval := ClampRefreshMinInterval(options.RefreshMinInterval)
	recentWindow := ClampRecentWindow(options.RecentWindow)
	server := &Server{
		reader:             options.Reader,
		namespace:          strings.TrimSpace(options.Namespace),
		watchName:          strings.TrimSpace(options.WatchName),
		refreshMinInterval: refreshMinInterval,
		recentWindow:       recentWindow,
		log:                options.Log,
	}
	server.snapshot = Snapshot{
		GeneratedAt:               time.Now().UTC(),
		Namespace:                 server.namespace,
		WatchName:                 server.watchName,
		RefreshMinIntervalSeconds: int64(refreshMinInterval.Seconds()),
		RecentWindowSeconds:       int64(recentWindow.Seconds()),
		Error:                     "status snapshot not generated yet",
	}
	return server
}

func ClampRefreshMinInterval(value time.Duration) time.Duration {
	if value <= 0 {
		value = DefaultRefreshMinInterval
	}
	if value < HardRefreshMinInterval {
		return HardRefreshMinInterval
	}
	return value
}

func ClampRecentWindow(value time.Duration) time.Duration {
	if value <= 0 {
		value = DefaultRecentWindow
	}
	if value < MinRecentWindow {
		return MinRecentWindow
	}
	if value > MaxRecentWindow {
		return MaxRecentWindow
	}
	return value
}

func (s *Server) SetReader(reader client.Reader) {
	s.reader = reader
}

func (s *Server) NeedLeaderElection() bool {
	return false
}

func (s *Server) Start(ctx context.Context) error {
	if s.log.GetSink() != nil {
		s.log.Info("status snapshotter started",
			"namespace", s.namespace,
			"watchName", s.watchName,
			"refreshMinInterval", s.refreshMinInterval.String(),
			"recentWindow", s.recentWindow.String(),
		)
	}
	s.refreshAndLog(ctx)
	ticker := time.NewTicker(s.refreshMinInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.refreshAndLog(ctx)
		}
	}
}

func (s *Server) ExtraHandlers() map[string]http.Handler {
	html := s.HTMLHandler()
	return map[string]http.Handler{
		"/status":      html,
		"/status/":     html,
		"/status.json": s.JSONHandler(),
	}
}

func (s *Server) HTMLHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(statusHTML))
	})
}

func (s *Server) JSONHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(s.Snapshot()); err != nil && s.log.GetSink() != nil {
			s.log.Error(err, "status snapshot encode failed")
		}
	})
}

func (s *Server) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *Server) Refresh(ctx context.Context) error {
	if s.reader == nil {
		s.setError("status reader is not configured")
		return nil
	}
	list := &v1alpha1.PgBouncerAuroraList{}
	if err := s.reader.List(ctx, list, client.InNamespace(s.namespace)); err != nil {
		s.setError(err.Error())
		return err
	}
	snapshot := Snapshot{
		GeneratedAt:               time.Now().UTC(),
		Namespace:                 s.namespace,
		WatchName:                 s.watchName,
		RefreshMinIntervalSeconds: int64(s.refreshMinInterval.Seconds()),
		RecentWindowSeconds:       int64(s.recentWindow.Seconds()),
		Resources:                 make([]ResourceStatus, 0, len(list.Items)),
	}
	for i := range list.Items {
		resource := &list.Items[i]
		if !matchesWatchName(s.watchName, resource.Name) {
			continue
		}
		status := resourceStatus(resource)
		snapshot.Resources = append(snapshot.Resources, status)
		addSummary(&snapshot.Summary, status)
	}
	sort.Slice(snapshot.Resources, func(i int, j int) bool {
		left := snapshot.Resources[i].Namespace + "/" + snapshot.Resources[i].Name
		right := snapshot.Resources[j].Namespace + "/" + snapshot.Resources[j].Name
		return left < right
	})
	snapshot.Summary.Clusters = len(snapshot.Resources)
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
	return nil
}

func (s *Server) refreshAndLog(ctx context.Context) {
	if err := s.Refresh(ctx); err != nil && s.log.GetSink() != nil {
		s.log.Error(err, "status snapshot refresh failed")
	}
}

func (s *Server) setError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.GeneratedAt = time.Now().UTC()
	s.snapshot.Namespace = s.namespace
	s.snapshot.WatchName = s.watchName
	s.snapshot.RefreshMinIntervalSeconds = int64(s.refreshMinInterval.Seconds())
	s.snapshot.RecentWindowSeconds = int64(s.recentWindow.Seconds())
	s.snapshot.Error = message
}

func resourceStatus(resource *v1alpha1.PgBouncerAurora) ResourceStatus {
	return ResourceStatus{
		Namespace:                    resource.Namespace,
		Name:                         resource.Name,
		State:                        resourceState(resource),
		Generation:                   resource.Generation,
		ObservedGeneration:           resource.Status.ObservedGeneration,
		TopologyHash:                 resource.Status.TopologyHash,
		MembershipHash:               resource.Status.MembershipHash,
		ConsecutiveDiscoveryFailures: resource.Status.ConsecutiveDiscoveryFailures,
		LastDiscoveryTime:            resource.Status.LastDiscoveryTime,
		LastMonitorTime:              resource.Status.LastMonitorTime,
		LastAppliedTime:              resource.Status.LastAppliedTime,
		LastKnownTopology:            resource.Status.LastKnownTopology,
		LastAppliedMembership:        resource.Status.LastAppliedMembership,
		ServiceSummary:               resource.Status.ServiceSummary,
		MissingInstances:             cloneMissingInstances(resource.Status.MissingInstances),
		Instances:                    cloneInstanceStatuses(resource.Status.Instances),
		Conditions:                   cloneConditions(resource.Status.Conditions),
	}
}

func addSummary(summary *Summary, resource ResourceStatus) {
	writerMembers := int(resource.ServiceSummary.Writer.Members)
	readerMembers := int(resource.ServiceSummary.Reader.Members)
	if writerMembers == 0 && len(resource.LastAppliedMembership.Writer) > 0 {
		writerMembers = len(resource.LastAppliedMembership.Writer)
	}
	if readerMembers == 0 && len(resource.LastAppliedMembership.Reader) > 0 {
		readerMembers = len(resource.LastAppliedMembership.Reader)
	}
	summary.Writers += writerMembers
	summary.Readers += readerMembers
	summary.DiscoveryFailureStreak += int(resource.ConsecutiveDiscoveryFailures)
	if conditionStatus(resource.Conditions, "Degraded") == metav1.ConditionTrue || resource.State == "Degraded" {
		summary.Degraded++
	}
	if conditionStatus(resource.Conditions, "Frozen") == metav1.ConditionTrue || resource.State == "Frozen" {
		summary.Frozen++
	}
	if resource.ServiceSummary.Reader.FallbackFromWriter {
		summary.ReaderFallbacks++
	}
}

func resourceState(resource *v1alpha1.PgBouncerAurora) string {
	if conditionStatus(resource.Status.Conditions, "Frozen") == metav1.ConditionTrue {
		return "Frozen"
	}
	if conditionStatus(resource.Status.Conditions, "Degraded") == metav1.ConditionTrue {
		return "Degraded"
	}
	if conditionStatus(resource.Status.Conditions, "Reconciled") == metav1.ConditionFalse {
		return "Degraded"
	}
	if resource.Status.ObservedGeneration < resource.Generation {
		return "Progressing"
	}
	return "Ready"
}

func conditionStatus(conditions []metav1.Condition, conditionType string) metav1.ConditionStatus {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status
		}
	}
	return metav1.ConditionUnknown
}

func matchesWatchName(watchName string, name string) bool {
	watchName = strings.TrimSpace(watchName)
	return watchName == "" || watchName == "*" || watchName == name
}

func cloneInstanceStatuses(in []v1alpha1.InstanceStatus) []v1alpha1.InstanceStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.InstanceStatus, len(in))
	copy(out, in)
	return out
}

func cloneMissingInstances(in []v1alpha1.MissingInstanceStatus) []v1alpha1.MissingInstanceStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.MissingInstanceStatus, len(in))
	copy(out, in)
	return out
}

func cloneConditions(in []metav1.Condition) []metav1.Condition {
	if len(in) == 0 {
		return nil
	}
	out := make([]metav1.Condition, len(in))
	copy(out, in)
	return out
}

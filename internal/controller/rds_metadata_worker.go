package controller

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	auroradiscovery "github.com/case-88/pgbouncer-aurora-operator/internal/discovery"
)

type RDSMetadataRefresher interface {
	RefreshCluster(ctx context.Context, clusterName string) (map[string]auroradiscovery.InstanceMetadata, error)
}

type RDSMetadataWorker struct {
	Client    client.Client
	Refresher RDSMetadataRefresher
	Namespace string
	WatchName string
	Interval  time.Duration
	Log       logr.Logger
}

func (w *RDSMetadataWorker) Start(ctx context.Context) error {
	if w.Client == nil || w.Refresher == nil || strings.TrimSpace(w.Namespace) == "" {
		return nil
	}
	if w.Log.GetSink() == nil {
		w.Log = ctrl.Log.WithName("rds-metadata")
	}
	interval := w.interval()
	w.Log.Info("rds metadata worker started",
		"namespace", w.Namespace,
		"watchName", w.WatchName,
		"interval", interval.String(),
	)
	w.refresh(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.refresh(ctx)
		}
	}
}

func (w *RDSMetadataWorker) NeedLeaderElection() bool {
	return true
}

func (w *RDSMetadataWorker) refresh(ctx context.Context) {
	clusters, err := w.clusterNames(ctx)
	if err != nil {
		w.Log.Error(err, "rds metadata managed CR list failed")
		return
	}
	if len(clusters) == 0 {
		w.Log.V(1).Info("rds metadata refresh skipped: no managed clusters")
		return
	}
	startedAt := time.Now()
	failures := 0
	for _, cluster := range clusters {
		if ctx.Err() != nil {
			return
		}
		if _, err := w.Refresher.RefreshCluster(ctx, cluster); err != nil {
			failures++
			w.Log.Error(err, "rds metadata cluster refresh failed", "cluster", cluster)
		}
	}
	w.Log.V(1).Info("rds metadata refresh completed",
		"clusters", len(clusters),
		"failures", failures,
		"duration", time.Since(startedAt).String(),
	)
}

func (w *RDSMetadataWorker) clusterNames(ctx context.Context) ([]string, error) {
	list := &v1alpha1.PgBouncerAuroraList{}
	if err := w.Client.List(ctx, list, client.InNamespace(w.Namespace)); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for i := range list.Items {
		resource := &list.Items[i]
		if !schedulerMatchesWatchName(w.WatchName, resource.Name) {
			continue
		}
		clusterName := strings.TrimSpace(resource.Spec.Discovery.ClusterName)
		if clusterName == "" {
			continue
		}
		seen[clusterName] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for clusterName := range seen {
		out = append(out, clusterName)
	}
	sort.Strings(out)
	return out, nil
}

func (w *RDSMetadataWorker) interval() time.Duration {
	if w.Interval > 0 {
		return w.Interval
	}
	return time.Minute
}

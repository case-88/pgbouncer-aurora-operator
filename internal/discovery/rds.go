package discovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RDSDescribeDBInstancesAPI interface {
	DescribeDBClusters(ctx context.Context, params *rds.DescribeDBClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error)
	DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
}

type WaitLimiter interface {
	Wait(ctx context.Context) error
}

type RDSMetadataResolver struct {
	Client      RDSDescribeDBInstancesAPI
	CacheTTL    time.Duration
	Now         func() time.Time
	RateLimiter WaitLimiter

	mu    sync.Mutex
	cache map[string]cachedInstanceMetadata
}

type cachedInstanceMetadata struct {
	metadata  InstanceMetadata
	cluster   map[string]InstanceMetadata
	expiresAt time.Time
}

func NewRDSMetadataResolver(ctx context.Context, region string) (*RDSMetadataResolver, error) {
	var options []func(*awsconfig.LoadOptions) error
	if region != "" {
		options = append(options, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &RDSMetadataResolver{Client: rds.NewFromConfig(cfg)}, nil
}

func (r *RDSMetadataResolver) Resolve(ctx context.Context, resource *v1alpha1.PgBouncerAurora, instanceNames []string) (map[string]InstanceMetadata, error) {
	logger := log.FromContext(ctx)
	if r.Client == nil {
		return nil, fmt.Errorf("rds client is nil")
	}
	clusterName := ""
	if resource != nil {
		clusterName = strings.TrimSpace(resource.Spec.Discovery.ClusterName)
	}
	if clusterName == "" {
		return nil, fmt.Errorf("discovery.clusterName is required for RDS metadata inventory")
	}
	now := r.now()
	ttl := r.cacheTTL(resource)
	r.prune(now)
	requestedNames := uniqueSorted(instanceNames)
	forceRefresh := reappearedInstanceRequested(resource, requestedNames)
	if !forceRefresh {
		if cached, ok := r.clusterCached(clusterName, requestedNames, now); ok {
			logger.V(1).Info("rds metadata cache hit",
				"cluster", clusterName,
				"instances", len(cached),
				"ttl", ttl.String(),
			)
			return cached, nil
		}
	} else {
		logger.V(1).Info("rds metadata cache bypassed",
			"cluster", clusterName,
			"reason", "previously missing instance reappeared",
		)
	}
	logger.V(1).Info("rds metadata refresh started",
		"cluster", clusterName,
		"requestedInstances", len(requestedNames),
		"ttl", ttl.String(),
	)
	startedAt := time.Now()
	if r.RateLimiter != nil {
		if err := r.RateLimiter.Wait(ctx); err != nil {
			logger.Error(err, "rds metadata rate limit wait failed",
				"cluster", clusterName,
				"duration", time.Since(startedAt).String(),
			)
			return nil, err
		}
	}
	if err := r.describeCluster(ctx, clusterName); err != nil {
		logger.Error(err, "rds cluster metadata refresh failed",
			"cluster", clusterName,
			"duration", time.Since(startedAt).String(),
		)
		return nil, err
	}
	resp, err := r.Client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		Filters: []types.Filter{{
			Name:   aws.String("db-cluster-id"),
			Values: []string{clusterName},
		}},
	})
	if err != nil {
		logger.Error(err, "rds metadata refresh failed",
			"cluster", clusterName,
			"duration", time.Since(startedAt).String(),
		)
		return nil, err
	}
	out := make(map[string]InstanceMetadata, len(resp.DBInstances))
	stored := 0
	uncacheable := 0
	cacheableCluster := true
	for _, instance := range resp.DBInstances {
		meta, ok := metadataFromDBInstance(instance)
		if !ok {
			continue
		}
		out[meta.InstanceName] = meta
		if cacheableMetadata(resource, meta) {
			r.store(nameCacheKey(clusterName, meta.InstanceName), meta, now.Add(ttl))
			stored++
		} else {
			cacheableCluster = false
			uncacheable++
		}
	}
	if cacheableCluster {
		r.storeCluster(clusterName, out, now.Add(ttl))
	}
	logger.V(1).Info("rds metadata refresh completed",
		"cluster", clusterName,
		"requestedInstances", len(requestedNames),
		"found", len(out),
		"stored", stored,
		"uncacheable", uncacheable,
		"duration", time.Since(startedAt).String(),
	)
	return out, nil
}

func (r *RDSMetadataResolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *RDSMetadataResolver) cacheTTL(resource ...*v1alpha1.PgBouncerAurora) time.Duration {
	if len(resource) > 0 && resource[0] != nil && resource[0].Spec.Discovery.MetadataRefreshInterval.Duration > 0 {
		return resource[0].Spec.Discovery.MetadataRefreshInterval.Duration
	}
	if r.CacheTTL > 0 {
		return r.CacheTTL
	}
	return time.Minute
}

func reappearedInstanceRequested(resource *v1alpha1.PgBouncerAurora, requestedNames []string) bool {
	if resource == nil || len(resource.Status.MissingInstances) == 0 || len(requestedNames) == 0 {
		return false
	}
	requested := map[string]bool{}
	for _, name := range requestedNames {
		requested[strings.TrimSpace(name)] = true
	}
	for _, missing := range resource.Status.MissingInstances {
		if requested[strings.TrimSpace(missing.InstanceName)] {
			return true
		}
	}
	return false
}

func (r *RDSMetadataResolver) describeCluster(ctx context.Context, clusterName string) error {
	resp, err := r.Client.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{DBClusterIdentifier: aws.String(clusterName)})
	if err != nil {
		return err
	}
	if len(resp.DBClusters) == 0 {
		return fmt.Errorf("DB cluster %q not found", clusterName)
	}
	return nil
}

func (r *RDSMetadataResolver) clusterCached(clusterName string, requestedNames []string, now time.Time) (map[string]InstanceMetadata, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		return nil, false
	}
	key := clusterCacheKey(clusterName)
	entry, ok := r.cache[key]
	if !ok || now.After(entry.expiresAt) {
		if ok {
			delete(r.cache, key)
		}
		return nil, false
	}
	out := map[string]InstanceMetadata{}
	for name, meta := range entry.cluster {
		out[name] = meta
	}
	for _, name := range requestedNames {
		if _, ok := out[name]; !ok {
			return nil, false
		}
	}
	return out, true
}

func (r *RDSMetadataResolver) cached(name string, now time.Time) (InstanceMetadata, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		return InstanceMetadata{}, false
	}
	entry, ok := r.cache[name]
	if !ok || now.After(entry.expiresAt) {
		if ok {
			delete(r.cache, name)
		}
		return InstanceMetadata{}, false
	}
	return entry.metadata, true
}

func (r *RDSMetadataResolver) store(name string, metadata InstanceMetadata, expiresAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]cachedInstanceMetadata{}
	}
	r.cache[name] = cachedInstanceMetadata{metadata: metadata, expiresAt: expiresAt}
}

func (r *RDSMetadataResolver) storeCluster(clusterName string, metadata map[string]InstanceMetadata, expiresAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]cachedInstanceMetadata{}
	}
	cluster := map[string]InstanceMetadata{}
	for name, meta := range metadata {
		cluster[name] = meta
	}
	r.cache[clusterCacheKey(clusterName)] = cachedInstanceMetadata{cluster: cluster, expiresAt: expiresAt}
}

func nameCacheKey(clusterName, instanceName string) string {
	return "instance:" + clusterName + ":" + instanceName
}

func clusterCacheKey(clusterName string) string {
	return "cluster:" + clusterName
}

func (r *RDSMetadataResolver) prune(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, entry := range r.cache {
		if now.After(entry.expiresAt) {
			delete(r.cache, name)
		}
	}
}

func metadataFromDBInstance(instance types.DBInstance) (InstanceMetadata, bool) {
	if instance.DBInstanceIdentifier == nil || *instance.DBInstanceIdentifier == "" {
		return InstanceMetadata{}, false
	}
	meta := InstanceMetadata{InstanceName: *instance.DBInstanceIdentifier}
	if instance.AvailabilityZone != nil {
		meta.AvailabilityZone = *instance.AvailabilityZone
	}
	if instance.DbiResourceId != nil {
		meta.DbiResourceId = *instance.DbiResourceId
	}
	return meta, true
}

func cacheableMetadata(resource *v1alpha1.PgBouncerAurora, metadata InstanceMetadata) bool {
	if resource == nil {
		return false
	}
	if !zoneAwareEnabled(resource) {
		return false
	}
	if metadata.AvailabilityZone == "" {
		return false
	}
	if metadata.DbiResourceId == "" {
		return false
	}
	return true
}

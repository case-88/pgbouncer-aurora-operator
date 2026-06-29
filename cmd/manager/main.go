package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/controller"
	auroradiscovery "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/discovery"
	pgmonitor "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/monitor"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/postgres"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/render"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/statuspage"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var awsRegion string
	var watchNamespace string
	var watchName string
	var maxConcurrentReconciles int
	var rdsMetadataCacheTTL time.Duration
	var resyncPeriod time.Duration
	var reconcileMinInterval time.Duration
	var awsAPIQPS float64
	var awsAPIBurst int
	var dbProbeQPS float64
	var dbProbeBurst int
	var monitorJobTimeout time.Duration
	var schedulerTick time.Duration
	var discoveryWorkers int
	var monitorWorkers int
	var statusRefreshMinInterval time.Duration
	var statusRecentWindow time.Duration
	var zapDevel bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&awsRegion, "aws-region", "", "AWS region for RDS metadata lookups.")
	flag.StringVar(&watchNamespace, "watch-namespace", "", "Namespace to watch. Defaults to WATCH_NAMESPACE. Empty is invalid.")
	flag.StringVar(&watchName, "watch-name", "", "PgBouncerAurora resource name to watch. Defaults to WATCH_NAME. Empty or * watches all resources in the namespace.")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 1, "Maximum number of concurrent PgBouncerAurora reconciles.")
	flag.DurationVar(&rdsMetadataCacheTTL, "rds-metadata-cache-ttl", time.Minute, "RDS DB instance metadata cache TTL. Values <=0 use the resolver default.")
	flag.DurationVar(&resyncPeriod, "resync-period", 0, "Controller cache resync period. Defaults to RESYNC_PERIOD or 60s.")
	flag.DurationVar(&reconcileMinInterval, "reconcile-min-interval", 0, "Minimum interval between heavy reconciles for the same PgBouncerAurora. Defaults to RECONCILE_MIN_INTERVAL or 1s.")
	flag.Float64Var(&awsAPIQPS, "aws-api-qps", 0, "Global AWS API rate limit QPS. Defaults to AWS_API_QPS or 2.")
	flag.IntVar(&awsAPIBurst, "aws-api-burst", 0, "Global AWS API rate limit burst. Defaults to AWS_API_BURST or 5.")
	flag.Float64Var(&dbProbeQPS, "db-probe-qps", 0, "Global DB probe rate limit QPS. Defaults to DB_PROBE_QPS or 20.")
	flag.IntVar(&dbProbeBurst, "db-probe-burst", 0, "Global DB probe rate limit burst. Defaults to DB_PROBE_BURST or 40.")
	flag.DurationVar(&monitorJobTimeout, "monitor-job-timeout", 0, "Whole monitor job timeout. Defaults to MONITOR_JOB_TIMEOUT or 8s.")
	flag.DurationVar(&schedulerTick, "scheduler-tick", 0, "Discovery/monitor scheduler tick. Defaults to SCHEDULER_TICK or 1s.")
	flag.IntVar(&discoveryWorkers, "discovery-workers", 0, "Number of discovery workers. Defaults to DISCOVERY_WORKERS or 2.")
	flag.IntVar(&monitorWorkers, "monitor-workers", 0, "Number of monitor workers. Defaults to MONITOR_WORKERS or 4.")
	flag.DurationVar(&statusRefreshMinInterval, "status-refresh-min-interval", 0, "Minimum interval for refreshing the cached /status snapshot. Defaults to STATUS_REFRESH_MIN_INTERVAL or 5s.")
	flag.DurationVar(&statusRecentWindow, "status-recent-window", 0, "Window for highlighting recently changed /status items. Defaults to STATUS_RECENT_WINDOW or 1m; clamped to 1m..24h.")
	flag.BoolVar(&zapDevel, "zap-devel", false, "Enable development-mode logging.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(zapDevel)))
	watchNamespace = effectiveWatchNamespace(watchNamespace)
	watchName = effectiveWatchName(watchName)
	if err := validateWatchNamespace(watchNamespace); err != nil {
		ctrl.Log.Error(err, "invalid watch namespace")
		os.Exit(1)
	}
	resyncPeriod, err := effectiveDuration(resyncPeriod, "RESYNC_PERIOD", time.Minute)
	if err != nil {
		ctrl.Log.Error(err, "invalid resync period")
		os.Exit(1)
	}
	reconcileMinInterval, err = effectiveDuration(reconcileMinInterval, "RECONCILE_MIN_INTERVAL", time.Second)
	if err != nil {
		ctrl.Log.Error(err, "invalid reconcile minimum interval")
		os.Exit(1)
	}
	awsAPIQPS, err = effectiveFloat(awsAPIQPS, "AWS_API_QPS", 2)
	if err != nil {
		ctrl.Log.Error(err, "invalid AWS API QPS")
		os.Exit(1)
	}
	awsAPIBurst, err = effectiveInt(awsAPIBurst, "AWS_API_BURST", 5)
	if err != nil {
		ctrl.Log.Error(err, "invalid AWS API burst")
		os.Exit(1)
	}
	dbProbeQPS, err = effectiveFloat(dbProbeQPS, "DB_PROBE_QPS", 20)
	if err != nil {
		ctrl.Log.Error(err, "invalid DB probe QPS")
		os.Exit(1)
	}
	dbProbeBurst, err = effectiveInt(dbProbeBurst, "DB_PROBE_BURST", 40)
	if err != nil {
		ctrl.Log.Error(err, "invalid DB probe burst")
		os.Exit(1)
	}
	monitorJobTimeout, err = effectiveDuration(monitorJobTimeout, "MONITOR_JOB_TIMEOUT", 8*time.Second)
	if err != nil {
		ctrl.Log.Error(err, "invalid monitor job timeout")
		os.Exit(1)
	}
	schedulerTick, err = effectiveDuration(schedulerTick, "SCHEDULER_TICK", time.Second)
	if err != nil {
		ctrl.Log.Error(err, "invalid scheduler tick")
		os.Exit(1)
	}
	discoveryWorkers, err = effectiveInt(discoveryWorkers, "DISCOVERY_WORKERS", 2)
	if err != nil {
		ctrl.Log.Error(err, "invalid discovery workers")
		os.Exit(1)
	}
	monitorWorkers, err = effectiveInt(monitorWorkers, "MONITOR_WORKERS", 4)
	if err != nil {
		ctrl.Log.Error(err, "invalid monitor workers")
		os.Exit(1)
	}
	statusRefreshMinInterval, err = effectiveStatusRefreshMinInterval(statusRefreshMinInterval)
	if err != nil {
		ctrl.Log.Error(err, "invalid status refresh minimum interval")
		os.Exit(1)
	}
	statusRecentWindow, err = effectiveStatusRecentWindow(statusRecentWindow)
	if err != nil {
		ctrl.Log.Error(err, "invalid status recent window")
		os.Exit(1)
	}
	cacheOptions := managerCacheOptions(watchNamespace, resyncPeriod)
	statusServer := statuspage.NewServer(statuspage.Options{
		Namespace:          watchNamespace,
		WatchName:          watchName,
		RefreshMinInterval: statusRefreshMinInterval,
		RecentWindow:       statusRecentWindow,
		Log:                ctrl.Log.WithName("status"),
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr, ExtraHandlers: statusServer.ExtraHandlers()},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "pgbouncer-aurora-operator.pgbouncer-aurora.io",
		Cache:                  cacheOptions,
		Client: client.Options{Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Secret{}},
		}},
	})
	if err != nil {
		ctrl.Log.Error(err, "unable to start manager")
		os.Exit(1)
	}
	statusServer.SetReader(mgr.GetClient())

	metadataResolver, err := auroradiscovery.NewRDSMetadataResolver(context.Background(), awsRegion)
	if err != nil {
		ctrl.Log.Error(err, "unable to initialize RDS metadata resolver; zone-aware AZ enrichment will be unavailable")
	} else if rdsMetadataCacheTTL > 0 {
		metadataResolver.CacheTTL = rdsMetadataCacheTTL
	}
	if metadataResolver != nil && awsAPIQPS > 0 && awsAPIBurst > 0 {
		metadataResolver.RateLimiter = rate.NewLimiter(rate.Limit(awsAPIQPS), awsAPIBurst)
	}
	dbFactory := postgres.SQLDBFactory{}
	var dbProbeLimiter *rate.Limiter
	if dbProbeQPS > 0 && dbProbeBurst > 0 {
		dbProbeLimiter = rate.NewLimiter(rate.Limit(dbProbeQPS), dbProbeBurst)
	}
	scheduleEvents := make(chan event.GenericEvent, 1024)
	reconciler := &controller.PgBouncerAuroraReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Discovery: auroradiscovery.Provider{
			Rows:     auroradiscovery.KubernetesRowSource{Client: mgr.GetClient(), DBFactory: dbFactory},
			Metadata: metadataResolver,
		},
		Monitor:                 pgmonitor.ProbeMonitor{Client: mgr.GetClient(), DBFactory: dbFactory, JobTimeout: monitorJobTimeout, ProbeLimiter: dbProbeLimiter},
		MaxConcurrentReconciles: maxConcurrentReconciles,
		WatchName:               watchName,
		ReconcileMinInterval:    reconcileMinInterval,
		ScheduleEvents:          scheduleEvents,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "unable to create controller")
		os.Exit(1)
	}
	if err := mgr.Add(&controller.Scheduler{
		Client:           mgr.GetClient(),
		APIReader:        mgr.GetAPIReader(),
		Discovery:        reconciler.Discovery,
		Monitor:          reconciler.Monitor,
		Events:           scheduleEvents,
		Namespace:        watchNamespace,
		WatchName:        watchName,
		Tick:             schedulerTick,
		DiscoveryWorkers: discoveryWorkers,
		MonitorWorkers:   monitorWorkers,
		Log:              ctrl.Log.WithName("scheduler"),
	}); err != nil {
		ctrl.Log.Error(err, "unable to add scheduler")
		os.Exit(1)
	}
	if err := mgr.Add(statusServer); err != nil {
		ctrl.Log.Error(err, "unable to add status snapshotter")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	ctrl.Log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func managerCacheOptions(watchNamespace string, resyncPeriod time.Duration) cache.Options {
	options := cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Pod{}: {
			Label: labels.SelectorFromSet(labels.Set{render.LabelManagedBy: render.ManagedByValue}),
		},
	}}
	namespace := strings.TrimSpace(watchNamespace)
	options.DefaultNamespaces = map[string]cache.Config{namespace: {}}
	if resyncPeriod > 0 {
		options.SyncPeriod = &resyncPeriod
	}
	return options
}

func effectiveWatchNamespace(flagValue string) string {
	if namespace := strings.TrimSpace(flagValue); namespace != "" {
		return namespace
	}
	return strings.TrimSpace(os.Getenv("WATCH_NAMESPACE"))
}

func effectiveWatchName(flagValue string) string {
	if name := strings.TrimSpace(flagValue); name != "" {
		return name
	}
	return strings.TrimSpace(os.Getenv("WATCH_NAME"))
}

func validateWatchNamespace(namespace string) error {
	if namespace == "" {
		return fmt.Errorf("WATCH_NAMESPACE or --watch-namespace must be set")
	}
	if namespace == metav1.NamespaceAll {
		return fmt.Errorf("cluster-wide watch is not supported")
	}
	if strings.Contains(namespace, ",") {
		return fmt.Errorf("multi-namespace watch is not supported: %q", namespace)
	}
	return nil
}

func effectiveDuration(flagValue time.Duration, envName string, defaultValue time.Duration) (time.Duration, error) {
	if flagValue > 0 {
		return flagValue, nil
	}
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", envName, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive: %s", envName, value)
	}
	return parsed, nil
}

func effectiveStatusRefreshMinInterval(flagValue time.Duration) (time.Duration, error) {
	value, err := effectiveDuration(flagValue, "STATUS_REFRESH_MIN_INTERVAL", statuspage.DefaultRefreshMinInterval)
	if err != nil {
		return 0, err
	}
	return statuspage.ClampRefreshMinInterval(value), nil
}

func effectiveStatusRecentWindow(flagValue time.Duration) (time.Duration, error) {
	value, err := effectiveDuration(flagValue, "STATUS_RECENT_WINDOW", statuspage.DefaultRecentWindow)
	if err != nil {
		return 0, err
	}
	return statuspage.ClampRecentWindow(value), nil
}

func effectiveFloat(flagValue float64, envName string, defaultValue float64) (float64, error) {
	if flagValue > 0 {
		return flagValue, nil
	}
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", envName, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive: %s", envName, value)
	}
	return parsed, nil
}

func effectiveInt(flagValue int, envName string, defaultValue int) (int, error) {
	if flagValue > 0 {
		return flagValue, nil
	}
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", envName, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive: %s", envName, value)
	}
	return parsed, nil
}

package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/render"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/statuspage"
)

func TestManagerCacheOptionsRestrictsNamespace(t *testing.T) {
	options := managerCacheOptions("pgbouncer-aurora", time.Minute)
	if _, ok := options.DefaultNamespaces["pgbouncer-aurora"]; !ok || len(options.DefaultNamespaces) != 1 {
		t.Fatalf("watch namespace should restrict cache namespace: %#v", options.DefaultNamespaces)
	}
	if options.SyncPeriod == nil || *options.SyncPeriod != time.Minute {
		t.Fatalf("sync period mismatch: %#v", options.SyncPeriod)
	}
	podOptions, ok := podCacheOptions(options.ByObject)
	if !ok || !podOptions.Label.Matches(labels.Set{render.LabelManagedBy: render.ManagedByValue}) {
		t.Fatalf("pod cache label selector missing: %#v", options.ByObject)
	}
}

func TestEffectiveWatchNamespacePrefersFlagThenEnv(t *testing.T) {
	t.Setenv("WATCH_NAMESPACE", "env-ns")
	if got := effectiveWatchNamespace(" flag-ns "); got != "flag-ns" {
		t.Fatalf("flag namespace should win: %q", got)
	}
	if got := effectiveWatchNamespace(""); got != "env-ns" {
		t.Fatalf("env namespace should be used: %q", got)
	}
}

func TestEffectiveWatchNamesPrefersFlagsThenEnv(t *testing.T) {
	t.Setenv("WATCH_NAMES", "env-a, env-b")
	if got := effectiveWatchNames(" flag-a, flag-b "); got != "flag-a,flag-b" {
		t.Fatalf("watch-names flag should win: %q", got)
	}
	if got := effectiveWatchNames(""); got != "env-a,env-b" {
		t.Fatalf("WATCH_NAMES env should be used: %q", got)
	}
	t.Setenv("WATCH_NAMES", "")
	if got := effectiveWatchNames(""); got != "*" {
		t.Fatalf("empty watch names should watch all: %q", got)
	}
	if got := effectiveWatchNames(" * "); got != "*" {
		t.Fatalf("star should be preserved: %q", got)
	}
}

func TestNormalizeWatchNames(t *testing.T) {
	if got := normalizeWatchNames(" db-a, db-b ,, db-a "); got != "db-a,db-b" {
		t.Fatalf("normalized watch names = %q", got)
	}
	if got := normalizeWatchNames("db-a,*"); got != "*" {
		t.Fatalf("star should watch all: %q", got)
	}
}

func TestWatchNameListFlagAppendsRepeatedValues(t *testing.T) {
	var flag watchNameListFlag
	if err := flag.Set("db-a, db-b"); err != nil {
		t.Fatal(err)
	}
	if err := flag.Set("db-c"); err != nil {
		t.Fatal(err)
	}
	if got := flag.String(); got != "db-a,db-b,db-c" {
		t.Fatalf("watch names flag = %q", got)
	}
	if err := flag.Set("*"); err != nil {
		t.Fatal(err)
	}
	if got := flag.String(); got != "*" {
		t.Fatalf("star should replace watch names: %q", got)
	}
}

func TestValidateWatchNamespace(t *testing.T) {
	for _, namespace := range []string{"", "a,b"} {
		if err := validateWatchNamespace(namespace); err == nil {
			t.Fatalf("namespace %q should be invalid", namespace)
		}
	}
	if err := validateWatchNamespace("db-pgbouncer"); err != nil {
		t.Fatalf("namespace should be valid: %v", err)
	}
}

func TestEffectiveDurationPrefersFlagThenEnvThenDefault(t *testing.T) {
	t.Setenv("RESYNC_PERIOD", "30s")
	if got, err := effectiveDuration(5*time.Second, "RESYNC_PERIOD", time.Minute); err != nil || got != 5*time.Second {
		t.Fatalf("flag duration should win: got=%s err=%v", got, err)
	}
	if got, err := effectiveDuration(0, "RESYNC_PERIOD", time.Minute); err != nil || got != 30*time.Second {
		t.Fatalf("env duration should be used: got=%s err=%v", got, err)
	}
	t.Setenv("RESYNC_PERIOD", "")
	if got, err := effectiveDuration(0, "RESYNC_PERIOD", time.Minute); err != nil || got != time.Minute {
		t.Fatalf("default duration should be used: got=%s err=%v", got, err)
	}
}

func TestEffectiveDurationRejectsInvalidEnv(t *testing.T) {
	t.Setenv("RESYNC_PERIOD", "nope")
	if _, err := effectiveDuration(0, "RESYNC_PERIOD", time.Minute); err == nil {
		t.Fatalf("invalid env duration should fail")
	}
	t.Setenv("RESYNC_PERIOD", "0s")
	if _, err := effectiveDuration(0, "RESYNC_PERIOD", time.Minute); err == nil {
		t.Fatalf("non-positive env duration should fail")
	}
}

func TestEffectiveStatusRefreshMinIntervalClampsTooSmallValues(t *testing.T) {
	t.Setenv("STATUS_REFRESH_MIN_INTERVAL", "1s")
	if got, err := effectiveStatusRefreshMinInterval(0); err != nil || got != statuspage.HardRefreshMinInterval {
		t.Fatalf("env duration should be clamped: got=%s err=%v", got, err)
	}
	if got, err := effectiveStatusRefreshMinInterval(time.Second); err != nil || got != statuspage.HardRefreshMinInterval {
		t.Fatalf("flag duration should be clamped: got=%s err=%v", got, err)
	}
	t.Setenv("STATUS_REFRESH_MIN_INTERVAL", "30s")
	if got, err := effectiveStatusRefreshMinInterval(0); err != nil || got != 30*time.Second {
		t.Fatalf("larger env duration should be preserved: got=%s err=%v", got, err)
	}
}

func TestEffectiveStatusRecentWindowClampsValues(t *testing.T) {
	t.Setenv("STATUS_RECENT_WINDOW", "30s")
	if got, err := effectiveStatusRecentWindow(0); err != nil || got != statuspage.MinRecentWindow {
		t.Fatalf("small env duration should be clamped: got=%s err=%v", got, err)
	}
	if got, err := effectiveStatusRecentWindow(30 * time.Second); err != nil || got != statuspage.MinRecentWindow {
		t.Fatalf("small flag duration should be clamped: got=%s err=%v", got, err)
	}
	t.Setenv("STATUS_RECENT_WINDOW", "15m")
	if got, err := effectiveStatusRecentWindow(0); err != nil || got != 15*time.Minute {
		t.Fatalf("env duration should be preserved: got=%s err=%v", got, err)
	}
	if got, err := effectiveStatusRecentWindow(48 * time.Hour); err != nil || got != statuspage.MaxRecentWindow {
		t.Fatalf("large flag duration should be clamped: got=%s err=%v", got, err)
	}
	t.Setenv("STATUS_RECENT_WINDOW", "")
	if got, err := effectiveStatusRecentWindow(0); err != nil || got != statuspage.DefaultRecentWindow {
		t.Fatalf("default duration should be used: got=%s err=%v", got, err)
	}
}

func TestEffectiveFloatPrefersFlagThenEnvThenDefault(t *testing.T) {
	t.Setenv("AWS_API_QPS", "3.5")
	if got, err := effectiveFloat(1.5, "AWS_API_QPS", 2); err != nil || got != 1.5 {
		t.Fatalf("flag float should win: got=%v err=%v", got, err)
	}
	if got, err := effectiveFloat(0, "AWS_API_QPS", 2); err != nil || got != 3.5 {
		t.Fatalf("env float should be used: got=%v err=%v", got, err)
	}
	t.Setenv("AWS_API_QPS", "")
	if got, err := effectiveFloat(0, "AWS_API_QPS", 2); err != nil || got != 2 {
		t.Fatalf("default float should be used: got=%v err=%v", got, err)
	}
}

func TestEffectiveIntPrefersFlagThenEnvThenDefault(t *testing.T) {
	t.Setenv("AWS_API_BURST", "7")
	if got, err := effectiveInt(3, "AWS_API_BURST", 5); err != nil || got != 3 {
		t.Fatalf("flag int should win: got=%v err=%v", got, err)
	}
	if got, err := effectiveInt(0, "AWS_API_BURST", 5); err != nil || got != 7 {
		t.Fatalf("env int should be used: got=%v err=%v", got, err)
	}
	t.Setenv("AWS_API_BURST", "")
	if got, err := effectiveInt(0, "AWS_API_BURST", 5); err != nil || got != 5 {
		t.Fatalf("default int should be used: got=%v err=%v", got, err)
	}
}

func podCacheOptions(byObject map[client.Object]cache.ByObject) (cache.ByObject, bool) {
	for object, options := range byObject {
		if _, ok := object.(*corev1.Pod); ok {
			return options, true
		}
	}
	return cache.ByObject{}, false
}

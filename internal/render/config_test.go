package render

import (
	"strings"
	"testing"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/internal/domain"
)

func TestPgBouncerINIConfigSectionsAndInstanceOverride(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.Discovery.Port = 5432
	spec.PgBouncer.Config = v1alpha1.PgBouncerConfigSpec{
		PgBouncer: map[string]string{"default_pool_size": "300", "listen_port": "16432", "pool_mode": "transaction"},
		Databases: map[string]map[string]string{
			"*":     {"pool_size": "300", "user": "svc"},
			"admin": {"user": "admin"},
		},
		Users: map[string]map[string]string{"svc": {"pool_size": "20"}},
		Peers: map[string]map[string]string{"peer-1": {"host": "pgbouncer-1", "port": "6432"}},
	}
	spec.PgBouncer.InstanceOverrides = []v1alpha1.InstanceOverrideSpec{{
		Name: "db-1",
		Config: v1alpha1.PgBouncerConfigSpec{
			PgBouncer: map[string]string{"default_pool_size": "100"},
			Databases: map[string]map[string]string{
				"*": {"pool_size": "100"},
			},
		},
	}}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example", Port: 5432})
	for _, expected := range []string{
		"* = host=db-1.example port=5432 pool_size=100 user=svc",
		"admin = host=db-1.example port=5432 user=admin",
		"auth_file = /etc/pgbouncer/userlist.txt",
		"listen_port = 16432",
		"pool_mode = transaction",
		"default_pool_size = 100",
		"[users]\nsvc = pool_size=20",
		"[peers]\npeer-1 = host=pgbouncer-1 port=6432",
	} {
		if !strings.Contains(ini, expected) {
			t.Fatalf("missing %q in:\n%s", expected, ini)
		}
	}
}

func TestPgBouncerINIDatabaseSectionProtectsManagedKeys(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.Discovery.Port = 5432
	spec.PgBouncer.Config.Databases = map[string]map[string]string{
		"*": {
			"host": "evil.example",
			"port": "15432",
			"user": "svc",
		},
	}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example"})
	for _, expected := range []string{
		"* = host=db-1.example port=5432 user=svc",
	} {
		if !strings.Contains(ini, expected) {
			t.Fatalf("missing %q in:\n%s", expected, ini)
		}
	}
	for _, unexpected := range []string{
		"host=evil.example",
		"port=15432",
	} {
		if strings.Contains(ini, unexpected) {
			t.Fatalf("unexpected %q in:\n%s", unexpected, ini)
		}
	}
}

func TestPgBouncerINIInjectsReservedProbeDatabase(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.Discovery.Port = 5432
	spec.Discovery.Database = "appdb"
	spec.PgBouncer.Config.Databases = map[string]map[string]string{
		"app": {
			"dbname": "appdb",
			"user":   "svc",
		},
		PgBouncerProbeDatabaseAlias: {
			"dbname": "evil",
			"user":   "svc",
		},
	}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example"})
	expected := PgBouncerProbeDatabaseAlias + " = host=db-1.example port=5432 dbname=appdb"
	if !strings.Contains(ini, expected) {
		t.Fatalf("missing %q in:\n%s", expected, ini)
	}
	for _, unexpected := range []string{
		PgBouncerProbeDatabaseAlias + " = host=db-1.example port=5432 dbname=evil",
		PgBouncerProbeDatabaseAlias + " = host=db-1.example port=5432 dbname=appdb user=svc",
	} {
		if strings.Contains(ini, unexpected) {
			t.Fatalf("unexpected %q in:\n%s", unexpected, ini)
		}
	}
}

func TestPgBouncerINIProbeDatabaseDefaultsToPostgres(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.Discovery.Port = 5432

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example"})
	expected := PgBouncerProbeDatabaseAlias + " = host=db-1.example port=5432 dbname=postgres"
	if !strings.Contains(ini, expected) {
		t.Fatalf("missing %q in:\n%s", expected, ini)
	}
}

func TestPgBouncerINISkipsProbeDatabaseWhenPathProbeDisabled(t *testing.T) {
	disabled := false
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.Monitor.PgBouncerPathProbe = &disabled
	spec.PgBouncer.Config.Databases = map[string]map[string]string{
		"*": {"user": "svc"},
	}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example", Port: 5432})
	if strings.Contains(ini, PgBouncerProbeDatabaseAlias) {
		t.Fatalf("unexpected probe alias in:\n%s", ini)
	}
}

func TestPgBouncerINIProtectsManagedKeysButAllowsConfigurableListenAddrAuthFileAndPidfile(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.PgBouncer.Config.PgBouncer = map[string]string{
		"listen_addr": "127.0.0.1",
		"listen_port": "15432",
		"auth_file":   "/tmp/userlist.txt",
		"auth_type":   "scram-sha-256",
		"pidfile":     "/tmp/pgbouncer.pid",
	}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example"})
	for _, expected := range []string{
		"listen_addr = 127.0.0.1",
		"listen_port = 15432",
		"auth_file = /tmp/userlist.txt",
		"auth_type = scram-sha-256",
		"pidfile = /tmp/pgbouncer.pid",
	} {
		if !strings.Contains(ini, expected) {
			t.Fatalf("missing %q in:\n%s", expected, ini)
		}
	}
	for _, unexpected := range []string{
		"listen_addr = 0.0.0.0",
		"auth_file = /etc/pgbouncer/userlist.txt",
	} {
		if strings.Contains(ini, unexpected) {
			t.Fatalf("unexpected %q in:\n%s", unexpected, ini)
		}
	}
}

func TestAuthFilePathUsesInstanceOverride(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.PgBouncer.Config.PgBouncer = map[string]string{"auth_file": "/base/userlist.txt"}
	spec.PgBouncer.InstanceOverrides = []v1alpha1.InstanceOverrideSpec{{
		Name: "db-1",
		Config: v1alpha1.PgBouncerConfigSpec{
			PgBouncer: map[string]string{"auth_file": "/override/userlist.txt"},
		},
	}}

	if got := AuthFilePath(spec, "db-1"); got != "/override/userlist.txt" {
		t.Fatalf("override auth file path = %q", got)
	}
	if got := AuthFilePath(spec, "db-2"); got != "/base/userlist.txt" {
		t.Fatalf("base auth file path = %q", got)
	}
}

func TestPgBouncerINIRendersArbitraryConfigKeysWithoutValidation(t *testing.T) {
	spec := v1alpha1.PgBouncerAuroraSpec{}
	spec.PgBouncer.Config.PgBouncer = map[string]string{
		"future_unknown_option": "enabled",
		"pool_mode":             "transaction",
	}
	spec.PgBouncer.Config.Databases = map[string]map[string]string{
		"*": {"custom_db_option": "value", "user": "svc"},
	}
	spec.PgBouncer.Config.Users = map[string]map[string]string{
		"svc": {"custom_user_option": "value"},
	}

	ini := PgBouncerINI(spec, domain.InstanceObservation{Name: "db-1", Endpoint: "db-1.example", Port: 5432})
	for _, expected := range []string{
		"future_unknown_option = enabled",
		"* = host=db-1.example port=5432 custom_db_option=value user=svc",
		"svc = custom_user_option=value",
	} {
		if !strings.Contains(ini, expected) {
			t.Fatalf("missing %q in:\n%s", expected, ini)
		}
	}
}

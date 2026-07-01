package render

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	v1alpha1 "github.com/case-88/pgbouncer-aurora-operator/api/v1alpha1"
	"github.com/case-88/pgbouncer-aurora-operator/internal/domain"
)

const (
	defaultAuthFilePath              = "/etc/pgbouncer/userlist.txt"
	defaultListenAddr                = "0.0.0.0"
	defaultPgBouncerAuthType         = "md5"
	defaultPgBouncerListenPort int32 = 6432
)

func PgBouncerINI(spec v1alpha1.PgBouncerAuroraSpec, instance domain.InstanceObservation) string {
	config := mergedConfig(spec, instance.Name)
	pgbouncerConfig := map[string]string{
		"auth_type":   defaultPgBouncerAuthType,
		"auth_file":   defaultAuthFilePath,
		"listen_addr": defaultListenAddr,
	}
	for key, value := range config.PgBouncer {
		pgbouncerConfig[key] = value
	}
	pgbouncerConfig["auth_file"] = authFilePathFromConfig(config)
	pgbouncerConfig["listen_port"] = fmt.Sprintf("%d", ListenPort(spec))

	var builder strings.Builder
	writeDatabasesSection(&builder, spec, instance, config.Databases)
	writeKeyValueSection(&builder, "pgbouncer", pgbouncerConfig)
	writeNamedKeyValueSection(&builder, "users", config.Users, nil)
	writeNamedKeyValueSection(&builder, "peers", config.Peers, nil)
	return strings.TrimRight(builder.String(), "\n") + "\n"
}

func ListenPort(spec v1alpha1.PgBouncerAuroraSpec) int32 {
	value := strings.TrimSpace(mergedConfig(spec, "").PgBouncer["listen_port"])
	if value == "" {
		return defaultPgBouncerListenPort
	}
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return defaultPgBouncerListenPort
	}
	return int32(port)
}

func AuthFilePath(spec v1alpha1.PgBouncerAuroraSpec, instanceName string) string {
	return authFilePathFromConfig(mergedConfig(spec, instanceName))
}

func authFilePathFromConfig(config v1alpha1.PgBouncerConfigSpec) string {
	value := strings.TrimSpace(config.PgBouncer["auth_file"])
	if value == "" {
		return defaultAuthFilePath
	}
	return value
}

func writeDatabasesSection(builder *strings.Builder, spec v1alpha1.PgBouncerAuroraSpec, instance domain.InstanceObservation, databases map[string]map[string]string) {
	builder.WriteString("[databases]\n")
	effectiveDatabases := make(map[string]map[string]string, len(databases)+1)
	if len(databases) == 0 {
		effectiveDatabases["*"] = map[string]string{}
	}
	for name, options := range databases {
		effectiveDatabases[name] = options
	}
	names := sortedNames(effectiveDatabases)
	for _, name := range names {
		builder.WriteString(databaseLine(name, spec, instance, effectiveDatabases[name]))
		builder.WriteByte('\n')
	}
	builder.WriteByte('\n')
}

func databaseLine(name string, spec v1alpha1.PgBouncerAuroraSpec, instance domain.InstanceObservation, options map[string]string) string {
	port := instance.Port
	if port == 0 {
		port = spec.Discovery.Port
	}
	if port == 0 {
		port = 5432
	}
	parts := []string{
		fmt.Sprintf("%s =", name),
		fmt.Sprintf("host=%s", instance.Endpoint),
		fmt.Sprintf("port=%d", port),
	}
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if isManagedDatabaseKey(key) {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, options[key]))
	}
	return strings.Join(parts, " ")
}

func writeKeyValueSection(builder *strings.Builder, name string, values map[string]string) {
	builder.WriteString("[")
	builder.WriteString(name)
	builder.WriteString("]\n")
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteString(" = ")
		builder.WriteString(values[key])
		builder.WriteByte('\n')
	}
	builder.WriteByte('\n')
}

func writeNamedKeyValueSection(builder *strings.Builder, section string, values map[string]map[string]string, ignored map[string]bool) {
	if len(values) == 0 {
		return
	}
	builder.WriteString("[")
	builder.WriteString(section)
	builder.WriteString("]\n")
	for _, name := range sortedNames(values) {
		builder.WriteString(name)
		builder.WriteString(" =")
		keys := sortedKeys(values[name])
		for _, key := range keys {
			if ignored != nil && ignored[strings.ToLower(key)] {
				continue
			}
			builder.WriteByte(' ')
			builder.WriteString(key)
			builder.WriteByte('=')
			builder.WriteString(values[name][key])
		}
		builder.WriteByte('\n')
	}
	builder.WriteByte('\n')
}

func mergedConfig(spec v1alpha1.PgBouncerAuroraSpec, instanceName string) v1alpha1.PgBouncerConfigSpec {
	config := deepCopyConfig(spec.PgBouncer.Config)
	for _, override := range spec.PgBouncer.InstanceOverrides {
		if override.Name != instanceName {
			continue
		}
		mergeConfigInto(&config, override.Config)
	}
	return config
}

func deepCopyConfig(in v1alpha1.PgBouncerConfigSpec) v1alpha1.PgBouncerConfigSpec {
	return v1alpha1.PgBouncerConfigSpec{
		PgBouncer: cloneMap(in.PgBouncer),
		Databases: cloneMapMap(in.Databases),
		Users:     cloneMapMap(in.Users),
		Peers:     cloneMapMap(in.Peers),
	}
}

func mergeConfigInto(base *v1alpha1.PgBouncerConfigSpec, overlay v1alpha1.PgBouncerConfigSpec) {
	mergeStringMapInto(&base.PgBouncer, overlay.PgBouncer)
	mergeStringMapMapInto(&base.Databases, overlay.Databases)
	mergeStringMapMapInto(&base.Users, overlay.Users)
	mergeStringMapMapInto(&base.Peers, overlay.Peers)
}

func mergeStringMapInto(base *map[string]string, overlay map[string]string) {
	if len(overlay) == 0 {
		return
	}
	if *base == nil {
		*base = map[string]string{}
	}
	for key, value := range overlay {
		(*base)[key] = value
	}
}

func mergeStringMapMapInto(base *map[string]map[string]string, overlay map[string]map[string]string) {
	if len(overlay) == 0 {
		return
	}
	if *base == nil {
		*base = map[string]map[string]string{}
	}
	for name, values := range overlay {
		current := cloneMap((*base)[name])
		if current == nil {
			current = map[string]string{}
		}
		for key, value := range values {
			current[key] = value
		}
		(*base)[name] = current
	}
}

func cloneMapMap(in map[string]map[string]string) map[string]map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[string]string, len(in))
	for key, value := range in {
		out[key] = cloneMap(value)
	}
	return out
}

func sortedNames(values map[string]map[string]string) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func isManagedDatabaseKey(key string) bool {
	switch strings.ToLower(key) {
	case "host", "port":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

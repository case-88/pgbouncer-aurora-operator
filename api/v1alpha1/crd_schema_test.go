package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestCRDRequiresRuntimeCriticalFields(t *testing.T) {
	data := readCRDManifest(t)
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}

	specSchema := objectAt(t, doc,
		"spec", "versions", 0, "schema", "openAPIV3Schema", "properties", "spec",
	)
	assertRequired(t, specSchema, "discovery", "pgbouncer")

	discovery := objectAt(t, specSchema, "properties", "discovery")
	assertRequired(t, discovery, "authSecretRef", "clusterName", "domainName")
	assertNoDefault(t, discovery)
	assertMinLength(t, objectAt(t, discovery, "properties", "clusterName"), 1)
	assertMinLength(t, objectAt(t, discovery, "properties", "domainName"), 1)
	assertMinLength(t, objectAt(t, discovery, "properties", "clusterEndpoints", "properties", "writer", "properties", "host"), 1)
	assertMinLength(t, objectAt(t, discovery, "properties", "clusterEndpoints", "properties", "reader", "properties", "host"), 1)
	assertRequired(t, objectAt(t, discovery, "properties", "authSecretRef"), "name")
	assertMinLength(t, objectAt(t, discovery, "properties", "authSecretRef", "properties", "name"), 1)
	sslMode := objectAt(t, discovery, "properties", "sslMode")
	assertDefault(t, sslMode, "require")
	assertEnum(t, sslMode, "disable", "allow", "prefer", "require", "verify-ca", "verify-full")

	pgbouncer := objectAt(t, specSchema, "properties", "pgbouncer")
	assertRequired(t, pgbouncer, "image", "authFileSecretRef")
	assertNoDefault(t, pgbouncer)
	assertMinLength(t, objectAt(t, pgbouncer, "properties", "image"), 1)
	assertRequired(t, objectAt(t, pgbouncer, "properties", "authFileSecretRef"), "name")
	assertMinLength(t, objectAt(t, pgbouncer, "properties", "authFileSecretRef", "properties", "name"), 1)

	services := objectAt(t, specSchema, "properties", "services")
	assertDefault(t, objectAt(t, services, "properties", "writer", "properties", "name"), "writer")
	assertDefault(t, objectAt(t, services, "properties", "reader", "properties", "name"), "reader")

	zoneAware := objectAt(t, specSchema, "properties", "topologyPolicy", "properties", "zoneAware")
	assertDefault(t, objectAt(t, zoneAware, "properties", "conflictPolicy"), "Warn")
	assertDefault(t, objectAt(t, specSchema, "properties", "topologyPolicy", "properties", "writerChangeConnectionHandling"), "RestartWriters")
}

func readCRDManifest(t *testing.T) []byte {
	t.Helper()
	paths := []string{
		filepath.Join("..", "..", "deploy", "crd.yaml"),
	}
	var lastErr error
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return data
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		lastErr = err
	}
	t.Fatalf("CRD manifest not found in %v: %v", paths, lastErr)
	return nil
}

func objectAt(t *testing.T, value any, path ...any) map[string]any {
	t.Helper()
	current := value
	for _, part := range path {
		switch key := part.(type) {
		case string:
			m, ok := current.(map[string]any)
			if !ok {
				t.Fatalf("path %v: expected object before %q, got %T", path, key, current)
			}
			current = m[key]
		case int:
			s, ok := current.([]any)
			if !ok || key >= len(s) {
				t.Fatalf("path %v: expected index %d in %T", path, key, current)
			}
			current = s[key]
		default:
			t.Fatalf("unsupported path segment %T", part)
		}
	}
	m, ok := current.(map[string]any)
	if !ok {
		t.Fatalf("path %v: expected object, got %T", path, current)
	}
	return m
}

func assertRequired(t *testing.T, schema map[string]any, names ...string) {
	t.Helper()
	required, ok := schema["required"].([]any)
	if !ok {
		t.Fatalf("schema has no required list: %#v", schema)
	}
	seen := make(map[string]bool, len(required))
	for _, item := range required {
		name, ok := item.(string)
		if !ok {
			t.Fatalf("required item is not string: %#v", item)
		}
		seen[name] = true
	}
	for _, name := range names {
		if !seen[name] {
			t.Fatalf("required field %q not found in %v", name, required)
		}
	}
}

func assertMinLength(t *testing.T, schema map[string]any, want int) {
	t.Helper()
	got, ok := schema["minLength"].(float64)
	if !ok || int(got) != want {
		t.Fatalf("minLength mismatch: got %#v want %d in %#v", schema["minLength"], want, schema)
	}
}

func assertNoDefault(t *testing.T, schema map[string]any) {
	t.Helper()
	if _, ok := schema["default"]; ok {
		t.Fatalf("required object schema must not set default: %#v", schema)
	}
}

func assertDefault(t *testing.T, schema map[string]any, want string) {
	t.Helper()
	got, ok := schema["default"].(string)
	if !ok || got != want {
		t.Fatalf("default mismatch: got %#v want %q in %#v", schema["default"], want, schema)
	}
}

func assertEnum(t *testing.T, schema map[string]any, names ...string) {
	t.Helper()
	values, ok := schema["enum"].([]any)
	if !ok {
		t.Fatalf("schema has no enum: %#v", schema)
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		name, ok := value.(string)
		if !ok {
			t.Fatalf("enum value is not string: %#v", value)
		}
		seen[name] = true
	}
	for _, name := range names {
		if !seen[name] {
			t.Fatalf("enum value %q not found in %v", name, values)
		}
	}
}

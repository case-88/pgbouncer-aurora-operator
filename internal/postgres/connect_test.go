package postgres

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCredentialsFromSecret(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"}, Data: map[string][]byte{
		"username": []byte("svc"),
		"password": []byte("pw"),
		"sslmode":  []byte("disable"),
	}}
	creds, err := CredentialsFromSecret(secret)
	if err != nil {
		t.Fatal(err)
	}
	if creds.Username != "svc" || creds.Password != "pw" || creds.SSLMode != "disable" {
		t.Fatalf("creds = %#v", creds)
	}
}

func TestCredentialsFromSecretDefaultsSSLMode(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"}, Data: map[string][]byte{
		"user":     []byte("svc"),
		"password": []byte("pw"),
	}}
	creds, err := CredentialsFromSecret(secret)
	if err != nil {
		t.Fatal(err)
	}
	if creds.SSLMode != "require" {
		t.Fatalf("sslmode = %s", creds.SSLMode)
	}
}

func TestConnString(t *testing.T) {
	dsn := ConnString(ConnInfo{Host: "db.example", Port: 5433, Database: "app", Username: "svc", Password: "p@w", SSLMode: "disable"})
	for _, expected := range []string{"postgres://svc:p%40w@db.example:5433/app", "sslmode=disable"} {
		if !strings.Contains(dsn, expected) {
			t.Fatalf("dsn %q missing %q", dsn, expected)
		}
	}
}

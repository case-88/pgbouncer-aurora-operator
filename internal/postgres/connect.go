package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Credentials struct {
	Username string
	Password string
	SSLMode  string
}

type ConnInfo struct {
	Host     string
	Port     int32
	Database string
	Username string
	Password string
	SSLMode  string
}

type DBFactory interface {
	Open(ctx context.Context, info ConnInfo) (*sql.DB, error)
}

type SQLDBFactory struct{}

func (SQLDBFactory) Open(ctx context.Context, info ConnInfo) (*sql.DB, error) {
	db, err := sql.Open("pgx", ConnString(info))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxIdleTime(30 * time.Second)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

func CredentialsFromSecret(secret *corev1.Secret) (Credentials, error) {
	if secret == nil {
		return Credentials{}, fmt.Errorf("secret is nil")
	}
	username := firstSecretValue(secret, "username", "user")
	password := firstSecretValue(secret, "password")
	sslMode := firstSecretValue(secret, "sslmode", "ssl_mode")
	if sslMode == "" {
		sslMode = "require"
	}
	if username == "" {
		return Credentials{}, fmt.Errorf("secret %s/%s missing username", secret.Namespace, secret.Name)
	}
	if password == "" {
		return Credentials{}, fmt.Errorf("secret %s/%s missing password", secret.Namespace, secret.Name)
	}
	return Credentials{Username: username, Password: password, SSLMode: sslMode}, nil
}

func ConnString(info ConnInfo) string {
	port := info.Port
	if port == 0 {
		port = 5432
	}
	database := info.Database
	if database == "" {
		database = "postgres"
	}
	sslMode := info.SSLMode
	if sslMode == "" {
		sslMode = "require"
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(info.Username, info.Password),
		Host:   info.Host + ":" + strconv.Itoa(int(port)),
		Path:   database,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String()
}

func firstSecretValue(secret *corev1.Secret, keys ...string) string {
	for _, key := range keys {
		if value, ok := secret.Data[key]; ok {
			return string(value)
		}
		if value, ok := secret.StringData[key]; ok {
			return value
		}
	}
	return ""
}

package discovery

import (
	"context"
	"database/sql"

	v1alpha1 "github.com/jongeun-lim-imweb-me/pgbouncer-aurora-operator/api/v1alpha1"
)

const AuroraReplicaStatusQuery = "select server_id, session_id from aurora_replica_status()"

type StandardQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func RowsFromStandardQueryer(ctx context.Context, db StandardQueryer) ([]AuroraReplicaStatusRow, error) {
	rows, err := db.QueryContext(ctx, AuroraReplicaStatusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuroraReplicaStatusRow{}
	for rows.Next() {
		var row AuroraReplicaStatusRow
		if err := rows.Scan(&row.ServerID, &row.SessionID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type SQLRowSource struct {
	DB Queryer
}

func (s SQLRowSource) Rows(ctx context.Context, _ *v1alpha1.PgBouncerAurora) ([]AuroraReplicaStatusRow, error) {
	rows, err := s.DB.QueryContext(ctx, AuroraReplicaStatusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuroraReplicaStatusRow{}
	for rows.Next() {
		var row AuroraReplicaStatusRow
		if err := rows.Scan(&row.ServerID, &row.SessionID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

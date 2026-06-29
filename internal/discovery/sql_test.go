package discovery

import (
	"context"
	"errors"
	"testing"
)

type fakeQueryer struct {
	query string
	rows  *fakeRows
	err   error
}

func (f *fakeQueryer) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	f.query = query
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

type fakeRows struct {
	items  []AuroraReplicaStatusRow
	idx    int
	closed bool
	err    error
}

func (r *fakeRows) Next() bool {
	return r.idx < len(r.items)
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.items[r.idx]
	r.idx++
	*(dest[0].(*string)) = row.ServerID
	*(dest[1].(*string)) = row.SessionID
	return nil
}

func (r *fakeRows) Err() error { return r.err }
func (r *fakeRows) Close() error {
	r.closed = true
	return nil
}

func TestSQLRowSourceRows(t *testing.T) {
	rows := &fakeRows{items: []AuroraReplicaStatusRow{{ServerID: "db-1", SessionID: MasterSessionID}}}
	queryer := &fakeQueryer{rows: rows}
	source := SQLRowSource{DB: queryer}
	out, err := source.Rows(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if queryer.query != AuroraReplicaStatusQuery {
		t.Fatalf("query = %s", queryer.query)
	}
	if !rows.closed {
		t.Fatalf("rows should be closed")
	}
	if len(out) != 1 || out[0].ServerID != "db-1" {
		t.Fatalf("rows = %#v", out)
	}
}

func TestSQLRowSourceQueryError(t *testing.T) {
	source := SQLRowSource{DB: &fakeQueryer{err: errors.New("boom")}}
	_, err := source.Rows(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}

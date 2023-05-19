package impl

import (
	"context"
	"database/sql"

	"github.com/assembly-hub/db"
)

// row func
type row struct {
	r *sql.Row
}

func (r *row) Scan(dest ...any) error {
	return r.r.Scan(dest...)
}

// rows func
type rows struct {
	rows *sql.Rows
}

func (rows *rows) ColumnTypes() ([]db.ColumnType, error) {
	types, err := rows.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	colType := make([]db.ColumnType, len(types))
	for _, col := range colType {
		colType = append(colType, col)
	}
	return colType, nil
}

func (rows *rows) Columns() ([]string, error) {
	return rows.rows.Columns()
}

func (rows *rows) Err() error {
	return rows.rows.Err()
}

func (rows *rows) Next() bool {
	return rows.rows.Next()
}

func (rows *rows) NextResultSet() bool {
	return rows.rows.NextResultSet()
}

func (rows *rows) Close() error {
	return rows.rows.Close()
}

func (rows *rows) Scan(dest ...any) error {
	return rows.rows.Scan(dest...)
}

// stmt func
type stmt struct {
	s *sql.Stmt
}

func (s *stmt) Close() error {
	return s.s.Close()
}

func (s *stmt) ExecContext(ctx context.Context, args ...any) (db.Result, error) {
	return s.s.ExecContext(ctx, args...)
}

func (s *stmt) QueryContext(ctx context.Context, args ...any) (db.Rows, error) {
	rs, err := s.s.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &rows{rs}, nil
}

func (s *stmt) QueryRowContext(ctx context.Context, args ...any) db.Row {
	r := s.s.QueryRowContext(ctx, args...)
	if r == nil {
		return r
	}
	return &row{r}
}

type sqlDB struct {
	db *sql.DB
}

func (db *sqlDB) PrepareContext(ctx context.Context, query string) (db.Stmt, error) {
	s, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &stmt{s}, nil
}

func (db *sqlDB) ExecContext(ctx context.Context, query string, args ...any) (db.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

func (db *sqlDB) QueryContext(ctx context.Context, query string, args ...any) (db.Rows, error) {
	rs, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &rows{rs}, nil
}

func (db *sqlDB) QueryRowContext(ctx context.Context, query string, args ...any) db.Row {
	r := db.db.QueryRowContext(ctx, query, args...)
	if r == nil {
		return nil
	}
	return &row{r}
}

func (db *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (db.Tx, error) {
	t, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &tx{t}, nil
}

func NewDB(db *sql.DB) db.Executor {
	return &sqlDB{
		db: db,
	}
}

func (db *sqlDB) GetRaw(ctx context.Context) *sql.DB {
	return db.db
}

// func
type tx struct {
	db *sql.Tx
}

func (db *tx) QueryRowContext(ctx context.Context, query string, args ...any) db.Row {
	r := db.db.QueryRowContext(ctx, query, args...)
	if r == nil {
		return nil
	}
	return &row{r}
}

func (db *tx) PrepareContext(ctx context.Context, query string) (db.Stmt, error) {
	s, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &stmt{s}, nil
}

func (db *tx) ExecContext(ctx context.Context, query string, args ...any) (db.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

func (db *tx) QueryContext(ctx context.Context, query string, args ...any) (db.Rows, error) {
	rs, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &rows{rs}, nil
}

func (db *tx) Commit() error {
	return db.db.Commit()
}

func (db *tx) Rollback() error {
	return db.db.Rollback()
}

func (db *tx) StmtContext(ctx context.Context, s db.Stmt) db.Stmt {
	newStmt := db.db.StmtContext(ctx, s.(*stmt).s)
	if s == nil {
		return nil
	}
	return &stmt{newStmt}
}

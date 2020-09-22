package db

import "database/sql"

type DB interface {
	Begin() (Txn, error)
	Close() error
	Exec(string) (*sql.Result, error)
	Query(string) (*sql.Rows, error)
}

type Txn interface {
	Exec(string) (*sql.Result, error)
	Query(string) (*sql.Rows, error)
	Commit() error
	Rollback() error
}

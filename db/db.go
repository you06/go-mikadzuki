package db

import "database/sql"

type DB interface {
	Begin() (Txn, error)
	Close() error
	Exec(string) (*sql.Result, error)
}

type Txn interface {
	Exec(string) (*sql.Result, error)
	Commit() error
	Rollback() error
}

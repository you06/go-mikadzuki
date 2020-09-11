package db

import "database/sql"

type DB interface {
	Begin() (Txn, error)
}

type Txn interface {
	Exec(string) (sql.Result, error)
	Commit() error
	Rollback() error
}

package db

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
)

type MySQL struct {
	dsn string
	db  *sql.DB
}

type MySQLTxn struct {
	txn *sql.Tx
}

func NewMySQL(dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &MySQL{
		dsn: dsn,
		db:  db,
	}, nil
}

func (m *MySQL) Begin() (Txn, error) {
	txn, err := m.db.Begin()
	return &MySQLTxn{txn}, errors.Trace(err)
}

func (m *MySQL) Close() error {
	return errors.Trace(m.db.Close())
}

func (m *MySQL) Exec(sql string) (*sql.Result, error) {
	r, err := m.db.Exec(sql)
	return &r, errors.Trace(err)
}

func (m *MySQLTxn) Exec(sql string) (*sql.Result, error) {
	r, err := m.txn.Exec(sql)
	return &r, errors.Trace(err)
}

func (m *MySQLTxn) Commit() error {
	return errors.Trace(m.txn.Commit())
}

func (m *MySQLTxn) Rollback() error {
	return errors.Trace(m.txn.Rollback())
}

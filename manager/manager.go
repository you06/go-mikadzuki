package manager

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/you06/go-mikadzuki/util"

	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/db"
	"github.com/you06/go-mikadzuki/graph"
	"github.com/you06/go-mikadzuki/kv"

	"github.com/juju/errors"
)

type Manager struct {
	cfg      *config.Config
	graphMgr graph.Generator
	db       db.DB
}

func NewManager(cfg *config.Config) *Manager {
	kvManager := kv.NewManager(&cfg.Global)
	m := Manager{
		cfg:      cfg,
		graphMgr: graph.NewGenerator(&kvManager, cfg),
	}
	return &m
}

func (m *Manager) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := m.Once(); err != nil {
				fmt.Println("mikadzuki failed", err)
				return
			}
		}
	}
}

func (m *Manager) Once() error {
	if err := m.initDB(); err != nil {
		return err
	}
	g := m.graphMgr.NewGraph(m.cfg.Global.Thread, m.cfg.Global.Action)
	logs := NewExecutionLog(m.cfg.Global.Thread, g.MaxAction())
	if m.cfg.Global.LogPath != "" {
		m.DumpGraph(g)
	}
	for _, stmt := range g.GetSchemas() {
		fmt.Println(stmt)
		if _, err := m.db.Exec(stmt); err != nil {
			return err
		}
	}
	txns := make([]db.Txn, m.cfg.Global.Thread)

	if err := g.IterateGraph(func(tID, aID int, tp graph.ActionTp, sqlStmt string) (*sql.Rows, *sql.Result, error) {
		var (
			rows *sql.Rows
			res  *sql.Result
			err  error
		)
		if tID >= 0 {
			logs.LogStart(tID, aID, tp, sqlStmt)
		}
		switch tp {
		case graph.Begin:
			txns[tID], err = m.db.Begin()
		case graph.Commit:
			if txns[tID] == nil {
				fmt.Printf("nil txn (%d, %d)\n", tID, aID)
			}
			err = txns[tID].Commit()
			txns[tID] = nil
		case graph.Rollback:
			if txns[tID] == nil {
				fmt.Printf("nil txn (%d, %d)\n", tID, aID)
			}
			err = txns[tID].Rollback()
			txns[tID] = nil
		case graph.Select:
			// -1 tID is for tracing bug
			if tID == -1 {
				rows, err = m.db.Query(sqlStmt)
				return rows, res, err
			} else {
				txn := txns[tID]
				util.AssertNotNil(txn)
				rows, err = txns[tID].Query(sqlStmt)
			}
		default:
			txn := txns[tID]
			util.AssertNotNil(txn)
			res, err = txn.Exec(sqlStmt)
		}
		if err != nil {
			logs.LogFail(tID, aID, err)
		} else {
			logs.LogSuccess(tID, aID)
		}
		return rows, res, err
	}); err != nil {
		if m.cfg.Global.LogPath != "" {
			m.DumpResult(logs)
		}
		return err
	} else {
		if m.cfg.Global.LogPath != "" {
			m.DumpResult(logs)
		}
	}
	if err := m.closeDB(); err != nil {
		fmt.Println("close DB failed", err)
	}
	return nil
}

func (m *Manager) initDB() error {
	var err error
	m.db, err = m.connectDB(m.cfg.Global.Target, m.cfg.Global.DSN)
	if err != nil {
		return errors.Trace(err)
	}
	dbname := m.cfg.Global.Database
	_, err = m.db.Exec(`SET @@GLOBAL.SQL_MODE="NO_ENGINE_SUBSTITUTION"`)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.db.Exec(fmt.Sprintf("DROP DATABASE %s", dbname))
	if err != nil && !strings.Contains(err.Error(), "database doesn't exist") {
		return errors.Trace(err)
	}
	_, err = m.db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		return errors.Trace(err)
	}
	m.db, err = m.connectDB(m.cfg.Global.Target, m.cfg.Global.DSN+dbname)
	return errors.Trace(err)
}

func (m *Manager) closeDB() error {
	if m.db == nil {
		return nil
	}
	return errors.Trace(m.db.Close())
}

func (m *Manager) connectDB(target, dsn string) (db.DB, error) {
	switch target {
	case "mysql":
		return db.NewMySQL(dsn)
	default:
		panic(fmt.Sprintf("Unsupported target %s", target))
	}
}

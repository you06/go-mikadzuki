package manager

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/graph"
	"github.com/you06/go-mikadzuki/kv"
)

type Manager struct {
	cfg      *config.Config
	graphMgr graph.Generator
}

func NewManager(cfg *config.Config) Manager {
	kvManager := kv.NewManager(&cfg.Global)
	return Manager{
		cfg:      cfg,
		graphMgr: graph.NewGenerator(&kvManager, cfg),
	}
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
	g := m.graphMgr.NewGraph(m.cfg.Global.Thread, m.cfg.Global.Action)

	if err := g.IterateGraph(func(tp graph.ActionTp, sql string) (*sql.Result, error) {
		return nil, nil
	}); err != nil {
		return err
	}
	return nil
}

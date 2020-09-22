package graph

import (
	"math/rand"

	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/kv"
)

type Generator struct {
	cfg          *config.Config
	globalConfig *config.Global
	graphConfig  *config.Graph
	dependConfig *config.Depend
	graphMap     map[ActionTp]int
	graphSum     int
	dependMap    map[DependTp]int
	dependSum    int
	kvManager    *kv.Manager
}

func NewGenerator(kvManager *kv.Manager, cfg *config.Config) Generator {
	generator := Generator{
		cfg:          cfg,
		globalConfig: &cfg.Global,
		graphConfig:  &cfg.Graph,
		dependConfig: &cfg.Depend,
		graphSum:     0,
		dependSum:    0,
		kvManager:    kvManager,
	}
	generator.CalcGraphSum()
	generator.CalcDependSum()
	return generator
}

func (g *Generator) CalcGraphSum() {
	graphMap := g.graphConfig.ToMap()
	g.graphMap = make(map[ActionTp]int, len(actionTps))
	g.graphSum = 0
	for _, tp := range actionTps {
		v := graphMap[string(tp)]
		g.graphMap[tp] = v
		g.graphSum += v
	}
}

func (g *Generator) CalcDependSum() {
	dependMap := g.dependConfig.ToMap()
	g.dependMap = make(map[DependTp]int, len(dependTps))
	g.dependSum = 0
	for _, tp := range dependTps {
		v := dependMap[string(tp)]
		g.dependMap[tp] = v
		g.dependSum += v
	}
}

func (g *Generator) randActionTp() ActionTp {
	rd := rand.Intn(g.graphSum)
	for tp, v := range g.graphMap {
		rd -= v
		if rd < 0 {
			return tp
		}
	}
	panic("unreachable")
}

func (g *Generator) randDependTp() DependTp {
	rd := rand.Intn(g.dependSum)
	for tp, v := range g.dependMap {
		rd -= v
		if rd < 0 {
			return tp
		}
	}
	panic("unreachable")
}

func (g *Generator) NewGraph(conn, length int) *Graph {
	g.kvManager.Reset()
	graph := NewGraph(g.kvManager, g.cfg)
	for i := 0; i < conn; i++ {
		timeline := graph.NewTimeline()
		for j := 0; j < length; j++ {
			// TODO: random txn status
			_ = timeline.NewTxnWithStatus(Committed)
		}
	}

	for i := 0; i < conn; i++ {
		graph.NewKV(i)
	}

	return graph
}

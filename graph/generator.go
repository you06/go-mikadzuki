package graph

import (
	"math/rand"

	"github.com/you06/go-mikadzuki/config"
)

type Generator struct {
	globalConfig *config.Global
	graphConfig  *config.Graph
	dependConfig *config.Depend
	graphMap     map[ActionTp]int
	graphSum     int
	dependMap    map[DependTp]int
	dependSum    int
}

func NewGenerator(global *config.Global, graph *config.Graph, depend *config.Depend) Generator {
	generator := Generator{
		globalConfig: global,
		graphConfig:  graph,
		dependConfig: depend,
		graphSum:     0,
		dependSum:    0,
	}
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

func (g *Generator) NewGraph(conn, length int) Graph {
	graph := NewGraph()
	for i := 0; i < conn; i++ {
		timeline := graph.NewTimeline()
		for j := 0; j < length; j++ {
			// start from begin and stop as commit
			if j == 0 {
				timeline.NewACtionWithTp(Begin)
			} else if j == length-1 {
				timeline.NewACtionWithTp(Commit)
			} else {
				timeline.NewACtionWithTp(g.randActionTp())
			}
		}
	}
	noDepend := graph.countNoDependAction()
	connectCnt := int(float64(noDepend) * g.globalConfig.DependRatio / 2)
	for i := 0; i < connectCnt; i++ {
		_ = graph.AddDependency(g.randDependTp())
	}
	return graph
}

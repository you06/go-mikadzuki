package graph

import (
	"fmt"
	"math/rand"

	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/kv"
)

type Generator struct {
	globalConfig *config.Global
	graphConfig  *config.Graph
	dependConfig *config.Depend
	graphMap     map[ActionTp]int
	graphSum     int
	dependMap    map[DependTp]int
	dependSum    int
	kvManager    *kv.Manager
}

func NewGenerator(kvManager *kv.Manager, global *config.Global, graph *config.Graph, depend *config.Depend) Generator {
	generator := Generator{
		globalConfig: global,
		graphConfig:  graph,
		dependConfig: depend,
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

func (g *Generator) NewGraph(conn, length int) Graph {
	g.kvManager.Reset()
	graph := NewGraph(g.kvManager)
	fmt.Println("make graph")
	for i := 0; i < conn; i++ {
		timeline := graph.NewTimeline()
		var (
			beforeTp ActionTp
			tp       ActionTp
		)
		for j := 0; j < length; j++ {
			// start from begin and stop as commit
			if j == 0 {
				tp = Begin
			} else if j == length-1 {
				if beforeTp == Commit || beforeTp == Rollback {
					break
				}
				tp = Commit
			} else {
				tp = Begin
				if beforeTp != Commit && beforeTp != Rollback {
					for tp == Begin {
						tp = g.randActionTp()
					}
				}
			}
			timeline.NewACtionWithTp(tp)
			beforeTp = tp
		}
	}
	fmt.Println("make dependency")
	// noDepend := graph.countNoDependAction()
	// connectCnt := int(float64(noDepend) * g.globalConfig.DependRatio / 2)
	failedCnt := 0
OUTER:
	for {
		failedCnt = 0
		for {
			if err := graph.AddDependency(g.randDependTp()); err != nil {
				failedCnt += 1
				if failedCnt >= MAX_RETRY {
					break OUTER
				}
			} else {
				break
			}
		}
	}
	// for i := 0; i < connectCnt; i++ {
	// 	_ = graph.AddDependency(g.randDependTp())
	// }
	fmt.Println("make linear kv")
	graph.MakeLinearKV()
	return graph
}

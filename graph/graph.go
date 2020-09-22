package graph

import (
	"strings"
	"time"

	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/kv"
)

const MAX_RETRY = 10
const WAIT_TIME = time.Millisecond

// Graph is the dependencies graph
// all the timelines should begin with `Begin` and end with `Commit` or `Rollback`
// the first transaction from each timeline should not depend on others
type Graph struct {
	cfg        *config.Config
	allocID    int
	timelines  []Timeline
	dependency int
	schema     *kv.Schema
	order      []ActionLoc
	graphMap   map[ActionTp]int
	graphSum   int
	dependMap  map[DependTp]int
	dependSum  int
}

type ActionLoc struct {
	tID int
	aID int
}

func NewGraph(kvManager *kv.Manager, cfg *config.Config) *Graph {
	g := Graph{
		cfg:        cfg,
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
		schema:     kvManager.NewSchema(),
		order:      []ActionLoc{},
	}
	g.CalcDependSum()
	g.CalcGraphSum()
	return &g
}

func (g *Graph) NewTimeline() *Timeline {
	id := g.allocID
	g.allocID += 1
	g.timelines = append(g.timelines, NewTimeline(id))
	return &g.timelines[id]
}

func (g *Graph) GetTimeline(n int) *Timeline {
	if n < g.allocID {
		return &g.timelines[n]
	}
	return nil
}

// func (g *Graph) TraceEmpty(action *Action, exec func(int, int, ActionTp, string) (*sql.Rows, *sql.Result, error)) {
// 	fmt.Printf("Executed SQL got empty: %s\n", action.SQL)
// 	fmt.Printf("Correct data of (%d, %d): %s\n", action.tID, action.id, g.schema.GetData(action.vID))
// 	for _, depend := range action.vIns {
// 		before := g.GetTimeline(depend.tID).GetAction(depend.aID)
// 		if before.vID == action.vID {
// 			fmt.Printf("It depends on (%d, %d, %s)\n", before.tID, before.id, before.tp)
// 		}
// 	}

// 	var b strings.Builder
// 	fmt.Print("Same key id actions:")
// 	ts := len(g.timelines)
// 	for i := 0; i < ts; i++ {
// 		t := g.GetTimeline(i)
// 		as := len(t.actions)
// 		for j := 0; j < as; j++ {
// 			a := t.GetAction(j)
// 			if a.kID == action.kID {
// 				fmt.Printf(" (%d, %d, %s)", a.tID, a.id, a.tp)
// 				if a.tp.IsWrite() && a.vID != kv.NULL_VALUE_ID {
// 					selectSQL := g.schema.SelectSQL(a.vID)
// 					rows, _, err := exec(-1, a.id, Select, selectSQL)
// 					if err == nil {
// 						if same, _ := g.schema.CompareData(a.vID, rows); same {
// 							fmt.Fprintf(&b, "(%d, %d, %s)'s value still alive, SQL: %s\n", a.tID, a.id, a.tp, selectSQL)
// 						}
// 					} else {
// 						fmt.Fprintf(&b, "(%d, %d, %s) exec error, error: %s\n", a.tID, a.id, a.tp, err.Error())
// 					}
// 				}
// 			}
// 		}
// 	}
// 	fmt.Print("\n")
// 	fmt.Print(b.String())
// }

func (g *Graph) String() string {
	var b strings.Builder
	for i, t := range g.timelines {
		if i != 0 {
			b.WriteString("\n")
		}
		b.WriteString(t.String())
	}
	return b.String()
}

func (g *Graph) GetSchemas() []string {
	return []string{g.schema.CreateTable()}
}

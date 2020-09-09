package graph

import (
	"math/rand"

	"github.com/juju/errors"
	"github.com/you06/go-mikadzuki/kv"
)

const MAX_RETRY = 10

// Graph is the dependencies graph
// all the timelines should begin with `Begin` and end with `Commit` or `Rollback`
// the first transaction from each timeline should not depend on others
type Graph struct {
	allocID    int
	timelines  []Timeline
	dependency int
	schema     *kv.Schema
}

func NewGraph(kvManager *kv.Manager) Graph {
	return Graph{
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
		schema:     kvManager.NewSchema(),
	}
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

func (g *Graph) AddDependency(dependTp DependTp) error {
	ts := len(g.timelines)
	if ts == 1 {
		return errors.New("connot connect between 1 timeline")
	}
	var (
		t1       int
		t2       int
		a1       int
		a2       int
		actions1 []Action
		actions2 []Action
		action1  Action
		action2  Action
	)
	for i := 0; i < MAX_RETRY; i++ {
		t1 = rand.Intn(ts)
		t2 = rand.Intn(ts)
		for t1 == t2 {
			t2 = rand.Intn(ts)
		}
		a1 = rand.Intn(len(g.timelines[t1].actions))
		actions1 = g.GetTransaction(t1, a1)
		a2 = rand.Intn(len(g.timelines[t2].actions))
		actions2 = g.GetTransaction(t2, a2)
		for actions2[0].id == 0 {
			a2 = rand.Intn(len(g.timelines[t2].actions))
			actions2 = g.GetTransaction(t2, a2)
		}
		if dependTp.CheckValidFrom(g.timelines[t1].actions[a1].tp) &&
			dependTp.CheckValidTo(g.timelines[t2].actions[a2].tp) {
			if dependTp.CheckValidLastFrom(actions1[len(actions1)-1].tp) &&
				dependTp.CheckValidLastTo(actions2[len(actions2)-1].tp) {
				action1 = dependTp.GetActionFrom(actions1)
				action2 = dependTp.GetActionTo(actions2)
				if !g.IfCycle(t1, action1.id, t2, action2.id) {
					g.ConnectTxnDepend(t1, action1.id, t2, action2.id, dependTp)
					g.ConnectValueDepend(t1, a1, t2, a2, dependTp)
					return nil
				}
			}
		}
		if i == MAX_RETRY-1 {
			return errors.New("cannot find suitable action pair for creating dependency")
		}
	}
	panic("unreachable")
}

func (g *Graph) GetTransaction(t, a int) []Action {
	timeline := g.timelines[t]
	l, r := a, a
	for {
		if timeline.actions[l].tp == Begin {
			break
		}
		if l == 0 {
			panic("txn begin not found")
		}
		l--
	}
	for {
		tp := timeline.actions[r].tp
		if tp == Commit || tp == Rollback {
			break
		}
		if r == len(timeline.actions)-1 {
			panic("txn begin not found")
		}
		r++
	}
	res := make([]Action, r-l+1)
	copy(res, timeline.actions[l-1:r])
	return res
}

func (g *Graph) IfCycle(t1, a1, t2, a2 int) bool {
	ts := len(g.timelines)
	visited := make([][]bool, ts)
	for i := 0; i < ts; i++ {
		length := len(g.timelines[i].actions)
		row := make([]bool, length)
		for j := 0; j < length; j++ {
			row[j] = false
		}
		visited[i] = row
	}
	var dfs func(t1, a1, t2, a2 int) bool
	dfs = func(t1, a1, t2, a2 int) bool {
		if t1 == t2 && a1 <= a2 {
			return true
		}
		if visited[t1][a1] {
			return false
		}
		visited[t1][a1] = true
		nexts := g.timelines[t1].actions[t2].outs
		for _, next := range nexts {
			if dfs(next.tID, next.aID, t2, a2) {
				return true
			}
		}
		return false
	}
	return dfs(t1, a1, t2, a2)
}

func (g *Graph) ConnectTxnDepend(t1, a1, t2, a2 int, tp DependTp) {
	from := g.GetTimeline(t1).GetAction(a1)
	to := g.GetTimeline(t2).GetAction(a2)
	// avoid adding duplicated dependency
	for _, out := range from.outs {
		if out.tID == t2 && out.aID == a2 && out.tp == tp {
			return
		}
	}
	from.outs = append(from.outs, Depend{
		tID: t2,
		aID: a2,
		tp:  tp,
	})
	to.ins = append(to.ins, Depend{
		tID: t1,
		aID: a1,
		tp:  tp,
	})
}

func (g *Graph) ConnectValueDepend(t1, a1, t2, a2 int, tp DependTp) {
	from := g.GetTimeline(t1).GetAction(a1)
	to := g.GetTimeline(t2).GetAction(a2)
	from.vOuts = append(from.vOuts, Depend{
		tID: t2,
		aID: a2,
		tp:  tp,
	})
	to.vIns = append(to.vIns, Depend{
		tID: t1,
		aID: a1,
		tp:  tp,
	})
}

func (g *Graph) countNoDependAction() int {
	cnt := 0
	for _, timeline := range g.timelines {
		for _, action := range timeline.actions {
			if action.tp != Begin &&
				action.tp != Commit &&
				action.tp != Rollback {
				if len(action.vIns) == 0 {
					cnt += 1
				}
			}
		}
	}
	return cnt
}

func (g *Graph) MakeLinearKV() {
	ts := len(g.timelines)
	progress := make([]int, ts)
	// initKVs for quick get Realtime dependent KV
	initKVs := make([]*kv.KV, ts)
	for i := 0; i < ts; i++ {
		initKVs[i] = g.schema.NewKV()
	}
	// init values for first transactions
	for i := 0; i < ts; i++ {
		timeline := g.GetTimeline(i)
		kv := initKVs[i]
		progress[i] = 0
		for {
			action := timeline.GetAction(progress[i])
			if action.tp == Commit || action.tp == Rollback {
				break
			}
			if action.tp.IsWrite() {
				action.kID = kv.ID
				switch action.tp {
				case Insert:
					if kv.Latest != -1 {
						kv = g.schema.NewKV()
					}
					kv.NewValue(g.schema)
					action.vID = kv.Latest
				case Update:
					kv.PutValue(g.schema)
					action.vID = kv.Latest
				case Delete:
					kv.DelValue(g.schema)
					action.vID = kv.Latest
				}
			} else if action.tp.IsRead() {
				action.kID = kv.ID
				action.vID = kv.Latest
			}
			progress[i] += 1
		}
	}

	for i := 0; i < ts; i++ {
		origin := progress[i]
		timeline := g.GetTimeline(i)
		progress[i] += 1
		deferCalc := false
		kv := initKVs[i]
		for {
			action := timeline.GetAction(progress[i])
			if len(action.vIns) > 0 {
				before := g.GetTimeline(action.vIns[0].tID).GetAction(action.vIns[0].aID)
				if before.kID == -1 {
					deferCalc = true
					break
				}
				if action.tp.IsRead() {
					action.vID = g.schema.KVs[before.kID].Latest
				} else if action.tp.IsWrite() {
					g.schema.KVs[before.kID].PutValue(g.schema)
				} else {
					panic("unreachable")
				}
			} else {
				if action.tp.IsRead() {
					action.kID = kv.ID
					action.vID = kv.Latest
				} else if action.tp.IsWrite() {
					switch action.tp {
					case Insert:
						kv = g.schema.NewKV()
						kv.NewValue(g.schema)
						action.kID = kv.ID
						action.vID = kv.Latest
					case Update:
						kv.PutValue(g.schema)
						action.vID = kv.Latest
					case Delete:
						kv.DelValue(g.schema)
						action.vID = kv.Latest
					}
				}
			}
		}
		// reset the process and get value later
		// the value already set will be overwritten
		if deferCalc {
			progress[i] = origin
		}
	}
}

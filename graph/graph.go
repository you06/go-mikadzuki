package graph

import (
	"math/rand"

	"github.com/juju/errors"
)

const MAX_RETRY = 10

type Graph struct {
	allocID    int
	timelines  []Timeline
	dependency int
}

func NewGraph() Graph {
	return Graph{
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
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
	for i := 0; i < MAX_RETRY; i++ {
		if err := g.TryAddDependency(dependTp); err != nil && i == MAX_RETRY-1 {
			return errors.Trace(err)
		} else {
			return nil
		}
	}
	panic("unreachable")
}

func (g *Graph) TryAddDependency(dependTp DependTp) error {
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
		a2 = rand.Intn(len(g.timelines[t2].actions))
		if dependTp.CheckValidFrom(g.timelines[t1].actions[a1].tp) &&
			dependTp.CheckValidTo(g.timelines[t2].actions[a2].tp) {
			actions1 = g.GetTransaction(t1, a1)
			actions2 = g.GetTransaction(t2, a2)
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

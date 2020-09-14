package graph

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/juju/errors"
	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
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
	order      []ActionLoc
}

type ActionLoc struct {
	tID int
	aID int
}

func NewGraph(kvManager *kv.Manager) Graph {
	return Graph{
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
		schema:     kvManager.NewSchema(),
		order:      []ActionLoc{},
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
		for j := 0; j < MAX_RETRY && actions2[0].id == 0; j++ {
			a2 = rand.Intn(len(g.timelines[t2].actions))
			actions2 = g.GetTransaction(t2, a2)
		}
		if actions2[0].id == 0 {
			return errors.New("failed to get non-first transaction")
		}
		if len(g.timelines[t1].actions[a1].vIns) == 0 &&
			len(g.timelines[t2].actions[a2].vIns) == 0 &&
			dependTp.CheckValidFrom(g.timelines[t1].actions[a1].tp) &&
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
	copy(res, timeline.actions[l:r+1])
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
		nexts := g.timelines[t1].actions[a1].outs
		for _, next := range nexts {
			if dfs(next.tID, next.aID, t2, a2) {
				return true
			}
		}
		return false
	}
	return dfs(t2, a2, t1, a1)
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
	fmt.Println(tp, "connect txn depend from", t1, a1, "to", t2, a2, "success")

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
	fmt.Println(tp, "connect value depend from", t1, a1, "to", t2, a2, "success")
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

func (g *Graph) PushOrder(tID, aID int) {
	g.order = append(g.order, ActionLoc{tID, aID})
}

func (g *Graph) getValueFromDepend(action *Action, txns *kv.Txns) (*kv.Txn, bool) {
	before := g.GetTimeline(action.vIns[0].tID).GetAction(action.vIns[0].aID)
	if !before.knowValue {
		return nil, false
	}
	action.knowValue = true
	g.PushOrder(action.tID, action.id)
	txn := g.schema.KVs[before.kID].Begin()
	util.AssertEQ(txn.KID, before.kID)
	if action.tp.IsRead() {
		action.kID = txn.KID
		action.vID = before.vID
		if before.vID == kv.INVALID_VALUE_ID {
			fmt.Println("[R] read invalid value from depend")
		}
		for _, t := range *txns {
			if t.KID == txn.KID {
				if t.Latest == kv.INVALID_VALUE_ID {
					fmt.Println("[R] read invalid value from inner")
				}
				action.vID = t.Latest
				return txn, true
			}
		}
		txn.Latest = before.vID
		txns.Push(txn)
	} else if action.tp.IsWrite() {
		switch action.tp {
		case Insert:
			// may use the old index value to make it a real dependency
			kv := g.schema.NewKV()
			txn = kv.Begin()
			action.SQL = txn.NewValue(g.schema)
		case Update:
			action.SQL = txn.PutValue(g.schema)
		case Delete:
			action.SQL = txn.DelValue(g.schema)
		}
		if txn.Latest == kv.INVALID_VALUE_ID {
			fmt.Println("[W] read invalid value from depend")
		}
		fmt.Printf("[W] assign value to %d, %d\n", action.tID, action.id)
		action.kID = before.kID
		action.vID = txn.Latest
		txns.Push(txn)
	} else {
		panic("unreachable")
	}
	util.AssertNE(action.vID, kv.INVALID_VALUE_ID)
	return txn, true
}

// MakeLinearKV assign values for actions via value dependencies
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
		txns := kv.Txns([]*kv.Txn{initKVs[i].Begin()})
		progress[i] = 0
		for {
			action := timeline.GetAction(progress[i])
			action.knowValue = true
			g.PushOrder(i, progress[i])
			if action.tp == Commit {
				txns.Commit()
				break
			} else if action.tp == Rollback {
				txns.Rollback()
				break
			}
			txn := txns.Rand()
			if action.tp.IsWrite() {
				switch action.tp {
				case Insert:
					if txn.Latest != kv.NULL_VALUE_ID {
						txn = g.schema.NewKV().Begin()
					}
					action.SQL = txn.NewValue(g.schema)
				case Update:
					action.SQL = txn.PutValue(g.schema)
				case Delete:
					action.SQL = txn.DelValue(g.schema)
				}
				action.kID = txn.KID
				action.vID = txn.Latest
			} else if action.tp.IsRead() {
				action.kID = txn.KID
				action.vID = txn.Latest
			}
			progress[i] += 1
		}
	}

	// txnsStore keeps transaction breaks by dependency in generation
	txnsStore := make(map[int]*kv.Txns)
	waitActions := make([]map[int]struct{}, ts)
	for i := 0; i < ts; i++ {
		waitActions[i] = make(map[int]struct{})
	}
	txnStatus := make([]ActionTp, ts)
	for {
		done := true
		for i := 0; i < ts; i++ {
			timeline := g.GetTimeline(i)
			var txns kv.Txns
			if t, ok := txnsStore[i]; ok {
				txns = *t
			} else {
				txns = []*kv.Txn{initKVs[i].Begin()}
				txnsStore[i] = &txns
			}
			if len(waitActions[i]) != 0 {
				var doneActionsIDs []int
				for id := range waitActions[i] {
					action := timeline.GetAction(id)
					// although we can suppose the txn added here is never used again
					// but add txn into txns break the internal order
					if _, ok := g.getValueFromDepend(action, &txns); ok {
						doneActionsIDs = append(doneActionsIDs, id)
						// switch txnStatus[i] {
						// case Commit:
						// 	t.Commit()
						// case Rollback:
						// 	t.Rollback()
						// }
						util.AssertNE(action.vID, kv.INVALID_VALUE_ID)
					}
				}
				for _, id := range doneActionsIDs {
					delete(waitActions[i], id)
				}
				if len(waitActions[i]) == 0 {
					delete(txnsStore, i)
				}
				done = false
				continue
			}
			if progress[i] == timeline.allocID-1 {
				continue
			}
			for {
				progress[i] += 1
				action := timeline.GetAction(progress[i])
				if action.tp == Commit {
					g.PushOrder(i, progress[i])
					txns.Commit()
					txnStatus[i] = Commit
					break
				} else if action.tp == Rollback {
					g.PushOrder(i, progress[i])
					txns.Rollback()
					txnStatus[i] = Rollback
					break
				} else if len(action.vIns) > 0 {
					if t, ok := g.getValueFromDepend(action, &txns); !ok {
						// this should be retry in next loop
						waitActions[i][progress[i]] = struct{}{}
						continue
					} else {
						util.AssertNE(action.vID, kv.INVALID_VALUE_ID)
						g.PushOrder(i, progress[i])
						txns.Push(t)
					}
				} else {
					g.PushOrder(i, progress[i])
					txn := txns.Rand()
					if action.tp.IsRead() {
						action.kID = txn.KID
						action.vID = txn.Latest
					} else if action.tp.IsWrite() {
						switch action.tp {
						case Insert:
							kv := g.schema.NewKV()
							txn = kv.Begin()
							txns.Push(txn)
							action.SQL = txn.NewValue(g.schema)
						case Update:
							action.SQL = txn.PutValue(g.schema)
						case Delete:
							action.SQL = txn.DelValue(g.schema)
						}
						action.kID = txn.KID
						action.vID = txn.Latest
					}
					action.knowValue = true
				}
			}
			if len(waitActions[i]) == 0 {
				delete(txnsStore, i)
			}
			done = false
		}
		if done {
			break
		}
	}

	fmt.Println(g.String())

	// generate all select SQLs and check if there are invalid value ids
	for i := 0; i < ts; i++ {
		timeline := g.GetTimeline(i)
		as := len(timeline.actions)
		for j := 0; j < as; j++ {
			action := timeline.GetAction(j)
			// if action == nil {
			// 	fmt.Println()
			// }
			if action.tp.IsWrite() {
				util.AssertNE(action.vID, kv.INVALID_VALUE_ID)
			} else if action.tp.IsRead() {
				switch action.tp {
				case Select:
					action.SQL = g.schema.SelectSQL(action.vID)
				}
			}
		}
	}
}

func (g *Graph) IterateGraph(exec func(tID int, tp ActionTp, sqlStmt string) (*sql.Result, error)) error {
	var (
		// res    *sql.Result
		err    error
		action *Action
	)
	for _, loc := range g.order {
		action = g.GetTimeline(loc.tID).GetAction(loc.aID)
		_, err = exec(loc.tID, action.tp, action.SQL)
		if err != nil {
			return err
		}
	}
	return nil
}

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

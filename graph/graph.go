package graph

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

const MAX_RETRY = 10
const WAIT_TIME = 2 * time.Millisecond

// Graph is the dependencies graph
// all the timelines should begin with `Begin` and end with `Commit` or `Rollback`
// the first transaction from each timeline should not depend on others
type Graph struct {
	cfg        *config.Config
	allocID    int
	timelines  []Timeline
	dependency int
	schema     *kv.Schema
	graphMap   map[ActionTp]int
	graphSum   int
	dependMap  map[DependTp]int
	dependSum  int
	ticker     util.Ticker
}

func NewGraph(kvManager *kv.Manager, cfg *config.Config) *Graph {
	g := Graph{
		cfg:        cfg,
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
		schema:     kvManager.NewSchema(),
		ticker:     util.NewTicker(time.Second),
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

func (g *Graph) GetTxn(tID, xID int) *Txn {
	timeline := g.GetTimeline(tID)
	if timeline == nil {
		return nil
	}
	return timeline.GetTxn(xID)
}

func (g *Graph) GetAction(tID, xID, aID int) *Action {
	timeline := g.GetTimeline(tID)
	if timeline == nil {
		return nil
	}
	return timeline.GetAction(xID, aID)
}

func (g *Graph) CalcGraphSum() {
	graphMap := g.cfg.Graph.ToMap()
	g.graphMap = make(map[ActionTp]int, len(actionTps))
	g.graphSum = 0
	for _, tp := range actionTps {
		v := graphMap[string(tp)]
		g.graphMap[tp] = v
		g.graphSum += v
	}
}

func (g *Graph) CalcDependSum() {
	dependMap := g.cfg.Depend.ToMap()
	g.dependMap = make(map[DependTp]int, len(dependTps))
	g.dependSum = 0
	for _, tp := range dependTps {
		v := dependMap[string(tp)]
		g.dependMap[tp] = v
		g.dependSum += v
	}
}

func (g *Graph) randActionTp() ActionTp {
	rd := rand.Intn(g.graphSum)
	for tp, v := range g.graphMap {
		rd -= v
		if rd < 0 {
			return tp
		}
	}
	panic("unreachable")
}

func (g *Graph) randDependTp() DependTp {
	rd := rand.Intn(g.dependSum)
	for tp, v := range g.dependMap {
		rd -= v
		if rd < 0 {
			return tp
		}
	}
	panic("unreachable")
}

func (g *Graph) NewKV(t int) {
	t = t % g.allocID
	pair := g.schema.NewKV()
	txn := g.GetTimeline(t).GetTxn(0)
	action := txn.NewActionWithTp(Insert)
	g.AssignPair(pair, action)
	g.Next(t, 0, 1, pair, action, 0)
}

// Next tries finding no cycle next dependent txn recursively
func (g *Graph) Next(t1, x1, x2 int, pair *kv.KV, before *Action, depth int) {
	var (
		tp       ActionTp
		dependTp DependTp
		t2       int
		txn      *Txn
		ifCycle  = false
		ok       bool
		path     [][2]int
		short    [][2]int
	)
	for i := 0; i < MAX_RETRY; i++ {
		tp = g.randActionTp()
		dependTp = DependTpFromActionTps(before.tp, tp)
		for dependTp == RR {
			tp = g.randActionTp()
			dependTp = DependTpFromActionTps(before.tp, tp)
		}
		t2, txn = g.RandTxnWithXID(x2)
		if txn == nil {
			return
		}
		for dependTp == WR && !txn.CanStartIn(before.tID, before.xID) {
			tp = g.randActionTp()
			dependTp = DependTpFromActionTps(before.tp, tp)
		}
		for j := 0; j < MAX_RETRY; j++ {
			if txn.abortByErr {
				t2, txn = g.RandTxnWithXID(x2)
			} else {
				break
			}
			if j == MAX_RETRY-1 {
				return
			}
		}

		if ok, path = g.IfCycle(t1, x1, t2, x2, dependTp); !ok {
			break
		} else if g.cfg.Global.Anomaly {
			short = shortPath(path)
			if canDeadlock(short) {
				realtimeCycle := false
				for x := 0; x < x1; x++ {
					if ok, _ := g.IfCycle(t1, x, t2, x2, dependTp); ok {
						realtimeCycle = true
						break
					}
				}
				if !realtimeCycle {
					ifCycle = true
					break
				}
			}
		}
		if i == MAX_RETRY-1 {
			return
		}
	}
	action := txn.NewActionWithTp(tp)
	if tp.IsRead() && action.vID == -1 {
		ifCycle = false
	}
	if !ifCycle {
		action.ins = append(action.ins, Depend{
			tID: t1,
			xID: x1,
			aID: before.id,
			tp:  dependTp,
		})
	}
	before.outs = append(before.outs, Depend{
		tID: t2,
		xID: x2,
		aID: action.id,
		tp:  dependTp,
	})
	if before.tp.IsRead() ||
		g.GetTimeline(before.tID).GetTxn(before.xID).status != Committed {
		action.beforeWrite = before.beforeWrite
	} else {
		action.beforeWrite = Depend{
			tID: t1,
			xID: x1,
			aID: before.id,
			tp:  WW,
		}
	}
	before.kvNext = &Depend{
		tID: t2,
		xID: x2,
		aID: action.id,
		tp:  dependTp,
	}
	if ifCycle {
		fmt.Println("cycle:", short)
		g.Anomaly(before, action, short)
	} else {
		g.ConnectTxn(t1, x1, t2, x2, dependTp)
		g.AssignPair(pair, action)
	}

	if util.RdBoolRatio(0.7 * float64(g.GetTimeline(t2).allocID) / float64(depth)) {
		g.Next(t2, x2, x2+util.RdRange(0, 2), pair, action, depth+1)
	}
	// if action.tp.IsWrite() && util.RdBoolRatio(2/float64(20+depth)) {
	// 	g.NextSplit(t2, x2, action, depth+1)
	// }
}

// func (g *Graph) NextSplit(t1, x1 int, before *Action, depth int) {
// 	var (
// 		pair = g.schema.NewKV()
// 		t2   int
// 		x2   int
// 		txn  *Txn
// 	)

// 	for i := 0; i < MAX_RETRY; i++ {
// 		util.AssertEQ(before.tp.IsWrite(), true)
// 		t2, x2, txn = g.RandTxn()
// 		if !g.IfCycle(t1, x1, t2, x2, WW) {
// 			break
// 		}
// 		if i == MAX_RETRY-1 {
// 			return
// 		}
// 	}
// 	action := txn.NewActionWithTp(Replace)
// 	action.SQL = pair.ReplaceNoTxn(g.schema, before.vID)
// 	action.kID = pair.ID
// 	action.vID = pair.Latest
// 	if util.RdBoolRatio(0.7 * float64(g.GetTimeline(t2).allocID) / float64(depth)) {
// 		g.Next(t2, x2, pair, action, depth+1)
// 	}
// }

// Anomaly generate anomaly so that cycle dependency can be interupted by error
// If there is an anomaly between txns, eg.
// T1 -> T2 -> T3 -> T1, and we want deadlock error occurs in T1 -> T2
// the dependency from T1 -> T2 must be executed after T2 -> T3 and T3 -> T1
// We always make deadlock because it's the only error can be expected.
// For read dependency, we simply use `SELECT FOR UPDATE` clause.
func (g *Graph) Anomaly(before, action *Action, short [][2]int) {
	// TODO: action's txn may be committed successfully, we can reuse lockKV
	lockKV := g.schema.NewKV()
	beforeSQL := lockKV.NewValueNoTxn(g.schema)
	beforeVID := lockKV.Latest
	afterSQL := lockKV.PutValueNoTxn(g.schema)
	beforeTxn := g.GetTimeline(before.tID).GetTxn(before.xID)
	actionTxn := g.GetTimeline(action.tID).GetTxn(action.xID)

	// beforeTxn.lockSQLs = append(beforeTxn.lockSQLs, beforeSQL)

	for _, depend := range actionTxn.endOuts {
		g.UnConnectTxn(actionTxn.tID, actionTxn.id, depend.tID, depend.xID, depend.tp)
	}

OUTER:
	for i := 0; i < beforeTxn.allocID; i++ {
		beforeAction := beforeTxn.GetAction(i)
		for _, depend := range beforeAction.outs {
			if beforeAction.tp.IsLock() && depend.tID == action.tID && depend.xID == action.xID {
				if actionTxn.GetAction(depend.aID).tp.IsLock() {
					before = beforeAction
					break OUTER
				}
			}
		}
		if beforeAction.kID == before.kID && beforeAction.tp.IsLock() {
			before = beforeAction
			break
		}
	}

	beforeID := before.id
	lockAction := g.InsertBefore(before.tID, before.xID, 0, Insert)
	lockAction.kID = lockKV.ID
	lockAction.vID = beforeVID
	lockAction.SQL = beforeSQL

	before = g.GetAction(before.tID, before.xID, beforeID+1)
	action.kID = lockKV.ID
	action.vID = lockKV.Latest
	action.SQL = afterSQL
	action.tp = Update
	action.mayAbortSelf = true
	action.abortBlock = &Depend{
		tID: before.tID,
		xID: before.xID,
		aID: before.id,
		tp:  WW,
	}
	var blockPoint map[int]int
	action.cycle, blockPoint = g.MakeCycle(short)
	action.cycle.Add(LocationFromAction(action))
	g.ConnectAction(before.tID, before.xID, blockPoint[before.tID], action.tID, action.xID, action.id, WW)
	fmt.Println("cycle done:", action.cycle.String())
}

func (g *Graph) MakeCycle(short [][2]int) (*Cycle, map[int]int) {
	cycle := EmptyCycle(g)
	blockPoint := make(map[int]int)

	for _, item := range short {
		txn := g.GetTxn(item[0], item[1])
		txn.AddNoStartIns(short)
	}
	for i := 0; i < len(short)-1; i++ {
		beforeTID := short[i][0]
		beforeXID := short[i][1]
		afterTID := short[i+1][0]
		afterXID := short[i+1][1]
		beforeTxn := g.GetTxn(beforeTID, beforeXID)
		afterTxn := g.GetTxn(afterTID, afterXID)
		util.AssertNotNil(beforeTxn)
		util.AssertNotNil(afterTxn)

		inShort := func(tID, xID int) bool {
			for _, item := range short {
				if tID == item[0] && xID == item[1] {
					return true
				}
			}
			return false
		}

		var ins []Depend
		for _, in := range afterTxn.startIns {
			if !inShort(in.tID, in.xID) {
				ins = append(ins, in)
				continue
			}
			for j := 0; j < afterTxn.allocID; j++ {
				action := afterTxn.GetAction(j)
				if !action.tp.IsRead() {
					continue
				}
				if action.beforeWrite == INVALID_DEPEND {
					continue
				}
				for inShort(action.beforeWrite.tID, action.beforeWrite.xID) {
					before := g.GetAction(action.beforeWrite.tID, action.beforeWrite.xID, action.beforeWrite.aID)
					action.beforeWrite = before.beforeWrite
				}
			}
		}
		afterTxn.startIns = ins

		for j := 0; j < beforeTxn.allocID; j++ {
			beforeAction := beforeTxn.GetAction(j)
			depend := INVALID_DEPEND
			for _, d := range beforeAction.outs {
				if d.tID == afterTID && d.xID == afterXID {
					depend = d
					break
				}
			}
			if depend == INVALID_DEPEND {
				if j == beforeTxn.allocID-1 {
					panic("dependency not found")
				}
				continue
			}
			afterAction := afterTxn.GetAction(depend.aID)
			if aID, ok := blockPoint[beforeTID]; ok && aID < beforeAction.id {
				// add new helper dependency
				// w(x, 1)
				// w(x, 2)(block here) -> w(y, 1)(unreachable)
				// w(y, 2)
				// will be changed to
				// w(x, 1)
				// w(z, 1) -> w(x, 2)(block here) -> w(y, 1)
				// w(z, 2)(block here) -> w(y, 2)
				//
				// w(y, 1) --WW-> w(y, 2) still exist after this
				lockKV := g.schema.NewKV()
				beforeSQL := lockKV.NewValueNoTxn(g.schema)
				beforeVID := lockKV.Latest
				afterSQL := lockKV.PutValueNoTxn(g.schema)
				helperFrom := g.InsertBefore(beforeTID, beforeXID, aID, Insert)
				helperFrom.kID = lockKV.ID
				helperFrom.vID = beforeVID
				helperFrom.tp = Insert
				helperFrom.SQL = beforeSQL
				helperTo := g.InsertBefore(afterTID, afterXID, afterAction.id, Update)
				helperTo.kID = lockKV.ID
				helperTo.vID = lockKV.Latest
				helperTo.tp = Update
				helperTo.SQL = afterSQL
				blockPoint[beforeTID] = aID + 1
				afterAction = helperTo
				g.ConnectAction(beforeTID, beforeXID, aID+1, afterTID, afterXID, afterAction.id, WW)

				// There is a harder way, we can chagne the dependency for same key, eg.
				// w(x, 1)
				// w(x, 2)(block here) -> w(x, 3)(unreachable)
				// w(x, 4)
				// will be changed to
				// w(x, 1)
				// w(x, 2)(block here) -> w(x, 3)(unreachable)
				// w(x, 4)(execute after w(x, 2), the dependency from w(x, 3) -> w(x, 4) will be considered later)

				// if beforeTxn.GetAction(aID).kID != beforeAction.kID {
				// } else {
				// 	// change dependency and make block pair
				// 	for i, in := range afterAction.ins {
				// 		if in.tID == beforeTID && in.xID == beforeXID {
				// 			afterAction.ins[i].aID = aID
				// 			break
				// 		}
				// 	}
				// 	cycle.AddBlockPair(RealtimeBlockPairFromActions(beforeAction, afterAction))
				// }
			} else {
				switch depend.tp {
				case WR:
					g.Select2SelectForUpdate(afterAction)
					g.UnConnectTxn(beforeTID, beforeXID, afterTID, afterXID, WR)
				case RW:
					g.Select2SelectForUpdate(beforeAction)
				}
			}
			afterAction.mayAbortSelf = true
			afterAction.cycle = &cycle
			cycle.Add(LocationFromAction(afterAction))
			blockPoint[afterTID] = afterAction.id
			break
		}
	}
	return &cycle, blockPoint
}

func (g *Graph) Select2SelectForUpdate(action *Action) {
	util.AssertIN(action.tp, []interface{}{Select, SelectForUpdate})
	if action.tp == SelectForUpdate {
		return
	}
	action.tp = SelectForUpdate
	pair := g.schema.GetKV(action.kID)
	if action.vID == kv.NULL_VALUE_ID && pair.DeleteVal != kv.INVALID_VALUE_ID {
		action.vID = pair.DeleteVal
	}
	action.SQL = pair.GetValueNoTxnForUpdateWithID(g.schema, action.vID)
}

func (g *Graph) InsertBefore(tID, xID, aID int, tp ActionTp) *Action {
	txn := g.GetTxn(tID, xID)
	action := txn.NewActionWithTp(tp)
	g.MoveBefore(tID, xID, aID, action.id)
	return txn.GetAction(aID)
}

func (g *Graph) MoveBefore(tID, xID, a1, a2 int) {
	txn := g.GetTxn(tID, xID)
	if a1 >= txn.allocID || a2 >= txn.allocID || a1 >= a2 {
		return
	}
	action := txn.actions[a2]
	action.id = a1
	if action.cycle != nil {
		action.cycle.Update(Location{
			tID: action.tID,
			xID: action.xID,
			aID: a2,
		}, LocationFromAction(&action))
	}
	for i := a2 - 1; i >= a1; i-- {
		txn.actions[i].id += 1
		txn.actions[i+1] = txn.actions[i]
		if txn.actions[i+1].cycle != nil {
			txn.actions[i+1].cycle.Update(Location{
				tID: txn.actions[i+1].tID,
				xID: txn.actions[i+1].xID,
				aID: txn.actions[i+1].id - 1,
			}, LocationFromAction(&txn.actions[i+1]))
		}
	}
	txn.actions[a1] = action
	inMap := make(map[Location]struct{})
	outMap := make(map[Location]struct{})
	for i := a1; i <= a2; i++ {
		action := txn.GetAction(i)

		for _, depend := range action.ins {
			location := LocationFromDepend(&depend)
			if _, ok := inMap[location]; ok {
				continue
			}
			before := g.GetAction(depend.tID, depend.xID, depend.aID)
			for i, out := range before.outs {
				if out.tID == tID && out.xID == xID &&
					out.aID >= a1 && out.aID <= a2 {
					if out.aID == a2 {
						before.outs[i].aID = a1
					} else {
						before.outs[i].aID += 1
					}
				}
			}
			if before.kvNext != nil &&
				before.kvNext.tID == tID && before.kvNext.xID == xID &&
				before.kvNext.aID >= a1 && before.kvNext.aID <= a2 {
				if before.kvNext.aID == a2 {
					before.kvNext.aID = a1
				} else {
					before.kvNext.aID += 1
				}
			}
			inMap[location] = struct{}{}
		}
		for _, depend := range action.outs {
			location := LocationFromDepend(&depend)
			if _, ok := outMap[location]; ok {
				continue
			}
			after := g.GetAction(depend.tID, depend.xID, depend.aID)
			for i, in := range after.ins {
				if in.tID == tID && in.xID == xID &&
					in.aID >= a1 && in.aID <= a2 {
					if in.aID == a2 {
						after.ins[i].aID = a1
					} else {
						after.ins[i].aID += 1
					}
				}
			}
			outMap[location] = struct{}{}
		}
	}
}

// Abort a txn
func (g *Graph) Abort(tID, xID int) {
	txn := g.GetTimeline(tID).GetTxn(xID)
	txn.status = Abort
	for i := 0; i < txn.allocID; i++ {
		action := txn.GetAction(i)
		// read abort does not effect
		if action.tp.IsRead() {
			continue
		}
		overwrite := action.beforeWrite
		beforeWrite := Depend{
			tID: action.tID,
			xID: action.xID,
			aID: action.id,
			tp:  WW,
		}
		pair := g.schema.GetKV(action.kID)
		for {
			if action.kvNext == nil {
				break
			}
			if action.beforeWrite == INVALID_DEPEND {
				break
			}
			next := g.GetTimeline(action.kvNext.tID).
				GetTxn(action.kvNext.xID).
				GetAction(action.kvNext.aID)
			if next.beforeWrite != beforeWrite {
				break
			}
			next.beforeWrite = overwrite
			action = next
			if action.tp.IsRead() {
				g.AssignPair(pair, action)
			}
		}
	}
}

func (g *Graph) AssignPair(pair *kv.KV, action *Action) {
	action.kID = pair.ID
	switch action.tp {
	case Select, SelectForUpdate:
		beforeVID := kv.NULL_VALUE_ID
		if action.beforeWrite != INVALID_DEPEND {
			beforeVID = g.GetTimeline(action.beforeWrite.tID).
				GetTxn(action.beforeWrite.xID).
				GetAction(action.beforeWrite.aID).vID
		}
		switch action.tp {
		case Select:
			action.SQL = pair.GetValueNoTxnWithID(g.schema, beforeVID)
		case SelectForUpdate:
			action.SQL = pair.GetValueNoTxnForUpdateWithID(g.schema, beforeVID)
		}
		action.vID = beforeVID
		return
	case Insert:
		if pair.Latest == kv.NULL_VALUE_ID {
			action.SQL = pair.NewValueNoTxn(g.schema)
		} else {
			action.tp = Replace
			action.SQL = pair.ReplaceNoTxn(g.schema, pair.Latest)
		}
	case Update:
		action.SQL = pair.PutValueNoTxn(g.schema)
	case Delete:
		action.SQL = pair.DelValueNoTxn(g.schema)
	default:
		panic(fmt.Sprintf("unsupport assign, ActionTp: %s", action.tp))
	}
	action.vID = pair.Latest
}

func (g *Graph) RandTxn() (int, int, *Txn) {
	t := rand.Intn(g.allocID)
	timeline := g.GetTimeline(t)
	x := rand.Intn(timeline.allocID)
	return t, x, timeline.GetTxn(x)
}

func (g *Graph) RandTxnWithXID(xID int) (int, *Txn) {
	t := rand.Intn(g.allocID)
	timeline := g.GetTimeline(t)
	return t, timeline.GetTxn(xID)
}

func (g *Graph) IfCycle(t1, x1, t2, x2 int, tp DependTp) (bool, [][2]int) {
	ts := len(g.timelines)
	visited := make([][]bool, ts)
	for i := 0; i < ts; i++ {
		length := 2 * len(g.timelines[i].txns)
		row := make([]bool, length)
		for j := 0; j < length; j++ {
			row[j] = false
		}
		visited[i] = row
	}

	getInOut := func(t, x int) ([]Depend, []Depend) {
		txn := g.GetTimeline(t).GetTxn(x / 2)
		if x%2 == 0 {
			return txn.startIns, txn.startOuts
		}
		return txn.endIns, txn.endOuts
	}

	var txn1Outs []Depend

	var dfs func(int, int, int, int, [][2]int) (bool, [][2]int)

	dfs = func(t1, x1, t2, x2 int, path [][2]int) (bool, [][2]int) {
		if !(x1 < 2*g.GetTimeline(t1).allocID) {
			return false, path
		}
		if t1 == t2 && x1 <= x2 {
			for i := x1; i <= x2; i++ {
				path = append(path, [2]int{t1, i})
			}
			return true, path
		}
		if visited[t1][x1] {
			return false, path
		}
		visited[t1][x1] = true
		for _, out := range txn1Outs {
			xID := 2 * out.xID
			if out.tp.toToEnd() {
				xID++
			}
			if out.tID == t1 && xID <= x1 {
				return false, path
			}
		}
		if ok, path := dfs(t1, x1+1, t2, x2, append(path, [2]int{t1, x1})); ok {
			return true, path
		}
		_, outs := getInOut(t1, x1)
		for _, out := range outs {
			xID := 2 * out.xID
			if out.tp.toToEnd() {
				xID += 1
			}
			if ok, path := dfs(out.tID, xID, t2, x2, append(path, [2]int{t1, x1})); ok {
				return true, path
			}
		}
		return false, path
	}

	var (
		cycle bool
		path  [][2]int
	)
	if t1 == t2 && x1 == x2 {
		return false, nil
	}
	switch tp {
	case WW:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		cycle, path = dfs(t2, 2*x2+1, t1, 2*x1+1, path)
	case WR:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		cycle, path = dfs(t2, 2*x2, t1, 2*x1+1, path)
	case RW:
		_, txn1Outs = getInOut(t1, 2*x1)
		cycle, path = dfs(t2, 2*x2+1, t1, 2*x1, path)
	default:
		panic("unreachable")
	}
	if !cycle {
		return cycle, nil
	}
	return cycle, path
}

func (g *Graph) ConnectAction(t1, x1, a1, t2, x2, a2 int, tp DependTp) {
	if t1 == t2 && x1 == x2 && a1 == a2 {
		return
	}
	action1 := g.GetAction(t1, x1, a1)
	action2 := g.GetAction(t2, x2, a2)
	depend1 := Depend{
		tID: t2,
		xID: x2,
		aID: a2,
		tp:  tp,
	}
	depend2 := Depend{
		tID: t1,
		xID: x1,
		aID: a1,
		tp:  tp,
	}
	action1.outs = append(action1.outs, depend1)
	action2.ins = append(action2.ins, depend2)
}

func (g *Graph) ConnectTxn(t1, x1, t2, x2 int, tp DependTp) {
	if t1 == t2 && x1 == x2 {
		return
	}
	txn1 := g.GetTxn(t1, x1)
	txn2 := g.GetTxn(t2, x2)
	depend1 := Depend{
		tID: t2,
		xID: x2,
		tp:  tp,
	}
	depend2 := Depend{
		tID: t1,
		xID: x1,
		tp:  tp,
	}
	if tp.toFromBegin() {
		txn1.startOuts = append(txn1.startOuts, depend1)
	} else {
		txn1.endOuts = append(txn1.endOuts, depend1)
	}
	if tp.toToBegin() {
		txn2.startIns = append(txn2.startIns, depend2)
	} else {
		txn2.endIns = append(txn2.endIns, depend2)
	}
}

func (g *Graph) UnConnectTxn(t1, x1, t2, x2 int, tp DependTp) {
	if t1 == t2 && x1 == x2 {
		return
	}
	txn2 := g.GetTimeline(t2).GetTxn(x2)
	startIns := []Depend{}
	endIns := []Depend{}
	for _, depend := range txn2.startIns {
		if depend.tID != t1 || depend.xID != x1 || depend.tp != tp {
			startIns = append(startIns, depend)
		}
	}
	for _, depend := range txn2.endIns {
		if depend.tID != t1 || depend.xID != x1 || depend.tp != tp {
			endIns = append(endIns, depend)
		}
	}
	txn2.startIns = startIns
	txn2.endIns = endIns
}

// IterateGraph goes over the graph and exec it by given sequence
// Since transaction is atomic, we only care about the WW value dependency here
// Commit/Rollback
//   i.   txns it RW depends on
//   ii.  itself
//   iii. Begin txns WR depend on it
//   iv.  Commit/Rollback txns WW depend on it
//   Commit/Rollback txns should check if there are Begin txns need to be waited before committing
//   should avoid any txns being committed between ii and iii, unless it will pollute value dependency
// Begin
//   i.   txns it WR depends on(only WR here)
//   ii.  itself
//   iii. txns RW depend on it(only RW here)
func (g *Graph) IterateGraph(exec func(int, ActionTp, string) (*sql.Rows, *sql.Result, error)) error {
	errCh := make(chan error)
	doneCh := make(chan struct{})
	var checkMutex sync.Mutex
	var txnMutex sync.Mutex
	var control sync.RWMutex
	progress := make([]int, len(g.timelines))
	ticker := util.NewTicker(time.Second)
	ticker.Go(func() {
		fmt.Println(progress)
	})
	for i := 0; i < g.allocID; i++ {
		progress[i] = 0
		go func(i int) {
			var (
				rows   *sql.Rows
				err    error
				txn    *Txn
				action *Action
			)
			timeline := g.GetTimeline(i)

			timeline.GetTxn(0).SetReady(true)
			for j := 0; j < timeline.allocID; j++ {
				progress[i]++
				ticker.Tick()
				txn = timeline.GetTxn(j)

				for _, depend := range txn.startIns {
					before := g.GetTimeline(depend.tID).GetTxn(depend.xID)
					t := 1
					for (depend.tp.toFromBegin() && !before.GetStart()) ||
						(depend.tp.toFromEnd() && !before.GetEnd()) {
						t += 1
						if t%1000 == 0 {
							fmt.Println("wait for txn start", txn.tID, txn.id)
						}
						time.Sleep(WAIT_TIME)
					}
				}

				txnMutex.Lock()
				if txn.allocID > 0 && !txn.GetStart() {
					if _, _, err = exec(i, Begin, "BEGIN"); err != nil {
						errCh <- err
						return
					}

					for _, sql := range txn.lockSQLs {
						_, _, err := exec(i, Insert, sql)
						if err != nil {
							errCh <- err
							return
						}
					}
					txn.SetStart(true)
				}
				txnMutex.Unlock()

				for k := 0; k < txn.allocID; k++ {
					progress[i]++
					ticker.Tick()
					control.RLock()
					action = txn.GetAction(k)
					control.RUnlock()
					if action.abortBlock == nil {
						// lock dependency
						if action.tp.IsLock() {
							for _, depend := range action.ins {
								before := g.GetTimeline(depend.tID).GetTxn(depend.xID).GetAction(depend.aID)
								if before.tp.IsLock() {
									t := 1
									for !before.GetExec() {
										t += 1
										if t%1000 == 0 {
											fmt.Println("wait fot lock dependency", action.tID, action.xID, action.id, action.mayAbortSelf, before.tID, before.xID, before.id, before.mayAbortSelf)
										}
										time.Sleep(WAIT_TIME)
									}
								}
							}
						}
						if action.tp.IsWrite() && action.beforeWrite != INVALID_DEPEND {
							depend := action.beforeWrite
							before := g.GetTimeline(depend.tID).GetTxn(depend.xID).GetAction(depend.aID)
							t := 1
							for !before.GetExec() {
								t += 1
								if t%1000 == 0 {
									fmt.Println("wait for ww", action.tID, action.xID, action.id, action.mayAbortSelf, before.tID, before.xID, before.id, before.mayAbortSelf)
								}
								time.Sleep(WAIT_TIME)
							}
						}
					} else {
						// wait for lock dependency
						before := g.GetTimeline(action.abortBlock.tID).
							GetTxn(action.abortBlock.xID).
							GetAction(action.abortBlock.aID)
						t := 1
						for !before.GetExec() {
							t += 1
							if t%1000 == 0 {
								fmt.Println("wait for locks", action.tID, action.xID, action.id, action.mayAbortSelf, before.tID, before.xID, before.id, before.mayAbortSelf)
							}
							time.Sleep(WAIT_TIME)
						}
					}

					execDone := make(chan struct{}, 1)
					go func() {
						ticker := time.NewTicker(time.Second)
						for range ticker.C {
							select {
							case <-execDone:
								return
							default:
								action.SetExec()
								fmt.Println("hang in exec", action.tID, action.xID, action.id)
							}
						}
					}()
					rows, _, err = exec(i, action.tp, action.SQL)
					execDone <- struct{}{}
					txnMutex.Lock()
					action.SetExec()
					action.SetDone()
					// end this transaction
					if action.mayAbortSelf {
						if err == nil && action.cycle.GetDone() && !action.cycle.GetErr() && !action.cycle.IfAbort() {
							errCh <- errors.Errorf("expect error: %s but got nil\ncycle: %s", DEADLOCK_ERROR_MESSAGE, action.cycle)
							return
						} else if err != nil && strings.Contains(err.Error(), DEADLOCK_ERROR_MESSAGE) {
							action.cycle.SetErr()
							action.cycle.SetDone()
							txnMutex.Unlock()
							g.Abort(txn.tID, txn.id)
							txn.SetEnd(true)
							for ; k < txn.allocID; k++ {
								action := txn.GetAction(k)
								action.SetDone()
							}
							if next := timeline.GetTxn(j + 1); next != nil {
								next.SetReady(true)
							}
							break
						}
					} else if err != nil {
						errCh <- err
						return
					}
					txnMutex.Unlock()
					switch action.tp {
					case Select:
						if same, err := g.schema.CompareData(action.vID, rows); !same {
							control.Lock()
							if strings.Contains(err.Error(), "data length 0, expect 1") {
								g.TraceEmpty(action, exec)
							}
							errCh <- fmt.Errorf("%s got %s", action.SQL, err.Error())
							control.Unlock()
						}
					}
					if rows != nil {
						rows.Close()
					}
					action.SetExec()
					if next := txn.GetAction(k + 1); next != nil {
						next.SetReady()
					}
				}
				progress[i]++

				for _, depend := range txn.endIns {
					before := g.GetTimeline(depend.tID).GetTxn(depend.xID)
					t := 1
					for (depend.tp.toFromBegin() && !before.GetStart()) ||
						(depend.tp.toFromEnd() && !before.GetEnd()) {
						next := g.GetTimeline(depend.tID).GetTxn(depend.xID)
						for !next.GetReady() {
							t += 1
							if t%1000 == 0 {
								fmt.Println("waiting for txn end", txn.tID, txn.id)
							}
							time.Sleep(WAIT_TIME)
						}
					}
				}
				txnMutex.Lock()
				if txn.allocID > 0 {
					if txn.status != Abort {
						if _, _, err := exec(txn.tID, txn.EndTp(), txn.EndSQL()); err != nil {
							errCh <- err
						}
					}
				}
				for _, depend := range txn.endIns {
					if depend.tp == WR {
						next := g.GetTimeline(depend.tID).GetTxn(depend.xID)
						if _, _, err := exec(depend.tID, Begin, "BEGIN"); err != nil {
							errCh <- err
						}
						next.SetStart(true)
					}
				}
				txn.SetEnd(true)
				if next := timeline.GetTxn(j + 1); next != nil {
					next.SetReady(true)
				}
				txnMutex.Unlock()
			}
			// check if all done
			checkMutex.Lock()
			defer checkMutex.Unlock()
			for i := 0; i < len(g.timelines); i++ {
				timeline := g.GetTimeline(i)
				if !timeline.GetTxn(timeline.allocID - 1).GetEnd() {
					return
				}
			}
			doneCh <- struct{}{}
		}(i)
	}

	for {
		select {
		case err := <-errCh:
			return err
		case <-doneCh:
			return nil
		}
	}
}

func (g *Graph) TraceEmpty(action *Action, exec func(int, ActionTp, string) (*sql.Rows, *sql.Result, error)) {
	fmt.Printf("Executed SQL got empty: %s\n", action.SQL)
	fmt.Printf("Correct data of (%d, %d, %d): %s\n", action.tID, action.xID, action.id, g.schema.GetData(action.vID))
	for _, depend := range action.ins {
		before := g.GetTimeline(depend.tID).GetTxn(depend.xID).GetAction(depend.aID)
		if before.vID == action.vID {
			fmt.Printf("It depends on (%d, %d, %d, %s)\n", before.tID, before.xID, before.id, before.tp)
		}
	}

	var b strings.Builder
	fmt.Print("Same key id actions:")
	for i := 0; i < g.allocID; i++ {
		t := g.GetTimeline(i)
		for j := 0; j < t.allocID; j++ {
			x := t.GetTxn(j)
			for k := 0; k < x.allocID; k++ {
				a := x.GetAction(k)
				if a.kID == action.kID {
					fmt.Printf(" (%d, %d, %d, %s)", a.tID, a.xID, a.id, a.tp)
					if a.tp.IsWrite() && a.vID != kv.NULL_VALUE_ID {
						selectSQL := g.schema.SelectSQL(a.vID)
						rows, _, err := exec(-1, Select, selectSQL)
						if err == nil {
							if same, _ := g.schema.CompareData(a.vID, rows); same {
								fmt.Fprintf(&b, "(%d, %d, %d, %s)'s value still alive, SQL: %s\n", a.tID, a.xID, a.id, a.tp, selectSQL)
							}
						} else {
							fmt.Fprintf(&b, "(%d, %d, %d, %s) exec error, error: %s\n", a.tID, a.xID, a.id, a.tp, err.Error())
						}
					}
				}
			}
		}
	}
	fmt.Print("\n")
	fmt.Print(b.String())
}

func (g *Graph) MaxAction() int {
	m := 0
	for i := 0; i < g.allocID; i++ {
		timeline := g.GetTimeline(i)
		c := 0
		for j := 0; j < timeline.allocID; j++ {
			c += 2
			c += timeline.GetTxn(j).allocID
		}
		if c > m {
			m = c
		}
	}
	return m
}

func (g *Graph) String() string {
	var b strings.Builder
	for i, t := range g.timelines {
		if i != 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(t.String())
	}
	return b.String()
}

func (g *Graph) GetSchemas() []string {
	return []string{g.schema.CreateTable()}
}

func shortPath(path [][2]int) [][2]int {
	var short [][2]int
	var idx = -1
	for i := 0; i < len(path); i++ {
		if idx == -1 {
			short = append(short, [2]int{path[i][0], path[i][1] / 2})
			idx++
			continue
		}
		if path[i][0] == short[idx][0] &&
			path[i][1]/2 == short[idx][1] {
			continue
		}
		short = append(short, [2]int{path[i][0], path[i][1] / 2})
		idx++
	}
	return short
}

func canDeadlock(short [][2]int) bool {
	m := make(map[int]struct{})
	for _, s := range short {
		if _, ok := m[s[0]]; ok {
			return false
		}
		m[s[0]] = struct{}{}
	}
	return true
}

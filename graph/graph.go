package graph

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

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
	order      []ActionLoc
	graphMap   map[ActionTp]int
	graphSum   int
	dependMap  map[DependTp]int
	dependSum  int
	ticker     util.Ticker
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
	pair := g.schema.NewKV()
	txn := g.GetTimeline(t).GetTxn(0)
	action := txn.NewActionWithTp(Insert)
	g.AssignPair(pair, action)
	g.Next(t, 0, pair, action, 0)
}

// Next tries finding no cycle next dependent txn recursively
func (g *Graph) Next(t1, x1 int, pair *kv.KV, before *Action, depth int) {
	var (
		tp       ActionTp
		dependTp DependTp
		t2       int
		x2       int
		txn      *Txn
	)
	for i := 0; i < MAX_RETRY; i++ {
		tp = g.randActionTp()
		dependTp = DependTpFromActionTps(before.tp, tp)
		for dependTp == RR {
			tp = g.randActionTp()
			dependTp = DependTpFromActionTps(before.tp, tp)
		}
		t2, x2, txn = g.RandTxn()
		if !g.IfCycle(t1, x1, t2, x2, dependTp) {
			break
		}
		if i == MAX_RETRY-1 {
			return
		}
	}
	action := txn.NewActionWithTp(tp)
	action.ins = append(action.ins, Depend{
		tID: t1,
		xID: x1,
		aID: before.id,
		tp:  dependTp,
	})
	before.outs = append(before.outs, Depend{
		tID: t2,
		xID: x2,
		aID: action.id,
		tp:  dependTp,
	})
	if before.tp.IsRead() {
		action.beforeWrite = before.beforeWrite
	} else {
		action.beforeWrite = Depend{
			tID: t1,
			xID: x1,
			aID: before.id,
			tp:  WW,
		}
	}
	g.AssignPair(pair, action)
	g.ConnectTxn(t1, x1, t2, x2, dependTp)
	if util.RdBoolRatio(0.7 * float64(g.GetTimeline(t2).allocID) / float64(depth)) {
		g.Next(t2, x2, pair, action, depth+1)
	}
	if action.tp.IsWrite() && util.RdBoolRatio(2/float64(20+depth)) {
		g.NextSplit(t2, x2, action, depth+1)
	}
}

func (g *Graph) NextSplit(t1, x1 int, before *Action, depth int) {
	var (
		pair = g.schema.NewKV()
		t2   int
		x2   int
		txn  *Txn
	)

	for i := 0; i < MAX_RETRY; i++ {
		util.AssertEQ(before.tp.IsWrite(), true)
		t2, x2, txn = g.RandTxn()
		if !g.IfCycle(t1, x1, t2, x2, WW) {
			break
		}
		if i == MAX_RETRY-1 {
			return
		}
	}
	action := txn.NewActionWithTp(Replace)
	action.SQL = pair.ReplaceNoTxn(g.schema, before.vID)
	action.kID = pair.ID
	action.vID = pair.Latest
	if util.RdBoolRatio(0.7 * float64(g.GetTimeline(t2).allocID) / float64(depth)) {
		g.Next(t2, x2, pair, action, depth+1)
	}
}

func (g *Graph) AssignPair(pair *kv.KV, action *Action) {
	switch action.tp {
	case Select, SelectForUpdate:
		action.SQL = pair.GetValueNoTxn(g.schema)
	case Insert:
		action.SQL = pair.NewValueNoTxn(g.schema)
	case Update:
		action.SQL = pair.PutValueNoTxn(g.schema)
	case Delete:
		action.SQL = pair.DelValueNoTxn(g.schema)
	default:
		panic(fmt.Sprintf("unsupport assign, ActionTp: %s", action.tp))
	}
	action.kID = pair.ID
	action.vID = pair.Latest
}

func (g *Graph) RandTxn() (int, int, *Txn) {
	t := rand.Intn(g.allocID)
	timeline := g.GetTimeline(t)
	x := rand.Intn(timeline.allocID)
	return t, x, timeline.GetTxn(x)
}

func (g *Graph) IfCycle(t1, x1, t2, x2 int, tp DependTp) bool {
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

	var dfs func(int, int, int, int) bool

	dfs = func(t1, x1, t2, x2 int) bool {
		if !(x1 < 2*g.GetTimeline(t1).allocID) {
			return false
		}
		if t1 == t2 && x1 <= x2 {
			return true
		}
		if visited[t1][x1] {
			return false
		}
		visited[t1][x1] = true
		for _, out := range txn1Outs {
			xID := 2 * out.xID
			if out.tp.toToEnd() {
				xID++
			}
			if out.tID == t1 && xID <= x1 {
				return false
			}
		}
		if dfs(t1, x1+1, t2, x2) {
			return true
		}
		_, outs := getInOut(t1, x1)
		for _, out := range outs {
			xID := 2 * out.xID
			if out.tp.toToEnd() {
				xID += 1
			}
			if dfs(out.tID, xID, t2, x2) {
				return true
			}
		}
		return false
	}

	switch tp {
	case WW:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		return dfs(t2, 2*x2+1, t1, 2*x1+1)
	case WR:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		return dfs(t2, 2*x2, t1, 2*x1+1)
	case RW:
		_, txn1Outs = getInOut(t1, 2*x1)
		return dfs(t2, 2*x2+1, t1, 2*x1)
	default:
		panic("unreachable")
	}
}

func (g *Graph) ConnectTxn(t1, x1, t2, x2 int, tp DependTp) {
	txn1 := g.GetTimeline(t1).GetTxn(x1)
	txn2 := g.GetTimeline(t2).GetTxn(x2)
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
func (g *Graph) IterateGraph(exec func(int, int, ActionTp, string) (*sql.Rows, *sql.Result, error)) error {
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
					for (depend.tp.toFromBegin() && !before.GetStart()) ||
						(depend.tp.toFromEnd() && !before.GetEnd()) {
						time.Sleep(WAIT_TIME)
					}
				}

				txnMutex.Lock()
				if txn.allocID > 0 && !txn.GetStart() {
					if _, _, err = exec(i, progress[i]-1, Begin, "BEGIN"); err != nil {
						errCh <- err
						return
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
					// WW value dependency
					if action.tp.IsWrite() && action.beforeWrite != INVALID_DEPEND {
						depend := action.beforeWrite
						for !g.GetTimeline(depend.tID).GetTxn(depend.xID).GetAction(depend.aID).GetExec() {
							time.Sleep(WAIT_TIME)
						}
					}

					rows, _, err = exec(i, progress[i]-1, action.tp, action.SQL)

					if err != nil {
						errCh <- err
						return
					}
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
					action.SetExec(true)
					if next := txn.GetAction(k + 1); next != nil {
						next.SetReady(true)
					}
				}
				progress[i]++

				for _, depend := range txn.endIns {
					before := g.GetTimeline(depend.tID).GetTxn(depend.xID)
					// waitTime := 1
					for (depend.tp.toFromBegin() && !before.GetStart()) ||
						(depend.tp.toFromEnd() && !before.GetEnd()) {
						next := g.GetTimeline(depend.tID).GetTxn(depend.xID)
						for !next.GetReady() {
							time.Sleep(WAIT_TIME)
						}
					}
				}
				txnMutex.Lock()
				if txn.allocID > 0 {
					if _, _, err := exec(txn.tID, progress[i]-1, txn.EndTp(), txn.EndSQL()); err != nil {
						errCh <- err
					}
				}
				for _, depend := range txn.endIns {
					if depend.tp == WR {
						next := g.GetTimeline(depend.tID).GetTxn(depend.xID)
						if _, _, err := exec(depend.tID, progress[depend.tID]-1, Begin, "BEGIN"); err != nil {
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

func (g *Graph) TraceEmpty(action *Action, exec func(int, int, ActionTp, string) (*sql.Rows, *sql.Result, error)) {
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
						rows, _, err := exec(-1, a.id, Select, selectSQL)
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
			b.WriteString("\n")
		}
		b.WriteString(t.String())
	}
	return b.String()
}

func (g *Graph) GetSchemas() []string {
	return []string{g.schema.CreateTable()}
}

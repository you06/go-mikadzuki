package graph

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

const DEADLOCK_ERROR_MESSAGE = "Deadlock"

type Txn struct {
	sync.RWMutex
	id         int
	tID        int
	allocID    int
	actions    []Action
	status     Status
	startOuts  []Depend
	startIns   []Depend
	noStartIns map[Depend]struct{}
	endIns     []Depend
	endOuts    []Depend
	ifStart    bool
	ifEnd      bool
	ifReady    bool
	abortByErr bool
	lockSQLs   []string
}

func NewTxn(id, tID int, s Status) Txn {
	return Txn{
		id:         id,
		tID:        tID,
		allocID:    0,
		actions:    []Action{},
		status:     s,
		startOuts:  []Depend{},
		startIns:   []Depend{},
		noStartIns: make(map[Depend]struct{}),
		endIns:     []Depend{},
		endOuts:    []Depend{},
		lockSQLs:   []string{},
	}
}

func (t *Txn) NewActionWithTp(actionTp ActionTp) *Action {
	id := t.allocID
	action := NewAction(id, t.tID, t.id, actionTp)
	t.allocID += 1
	t.actions = append(t.actions, action)
	return &t.actions[id]
}

func (t *Txn) GetAction(n int) *Action {
	if n < t.allocID {
		return &t.actions[n]
	}
	return nil
}

func (t *Txn) SetStart(r bool) {
	t.Lock()
	t.ifStart = r
	t.Unlock()
}

func (t *Txn) GetStart() bool {
	t.RLock()
	defer t.RUnlock()
	return t.ifStart
}

func (t *Txn) SetEnd(r bool) {
	t.Lock()
	t.ifEnd = r
	t.Unlock()
}

func (t *Txn) GetEnd() bool {
	t.RLock()
	defer t.RUnlock()
	return t.ifEnd
}

func (t *Txn) SetReady(r bool) {
	t.Lock()
	t.ifReady = r
	t.Unlock()
}

func (t *Txn) GetReady() bool {
	t.RLock()
	defer t.RUnlock()
	return t.ifReady
}

func (t *Txn) String() string {
	var b strings.Builder
	b.WriteString("Begin")
	for _, depend := range t.startIns {
		fmt.Fprintf(&b, "[%d, %d]", depend.tID, depend.xID)
	}
	for i := 0; i < t.allocID; i++ {
		b.WriteString(" -> ")
		b.WriteString(t.actions[i].String())
	}
	b.WriteString(" -> ")
	switch t.status {
	case Committed:
		b.WriteString("Commit")
	case Rollbacked:
		b.WriteString("Rollback")
	case Abort:
		b.WriteString("Abort")
	}
	for _, depend := range t.endIns {
		fmt.Fprintf(&b, "[%d, %d]", depend.tID, depend.xID)
	}
	return b.String()
}

func (t *Txn) EndTp() ActionTp {
	switch t.status {
	case Committed:
		return Commit
	case Rollbacked:
		return Rollback
	default:
		panic(fmt.Sprintf("unsupport txn status %s", t.status))
	}
}

func (t *Txn) EndSQL() string {
	switch t.status {
	case Committed:
		return "COMMIT"
	case Rollbacked:
		return "ROLLBACK"
	default:
		panic(fmt.Sprintf("unsupport txn status %s", t.status))
	}
}

func (t *Txn) AddNoStartIns(path [][2]int) {
	for _, d := range path {
		t.noStartIns[Depend{tID: d[0], xID: d[1]}] = struct{}{}
	}
}

func (t *Txn) CanStartIn(tID, xID int) bool {
	_, ok := t.noStartIns[Depend{tID: tID, xID: xID}]
	return !ok
}

type Location struct {
	tID int
	xID int
	aID int
}

func LocationFromDepend(depend *Depend) Location {
	return Location{
		tID: depend.tID,
		xID: depend.xID,
		aID: depend.aID,
	}
}

func LocationFromAction(action *Action) Location {
	return Location{
		tID: action.tID,
		xID: action.xID,
		aID: action.id,
	}
}

type Cycle struct {
	// execution phase
	// 0: not done
	// 1: done
	phase int64
	// has error?
	// 0: not yet
	// 1: has
	err                int64
	graph              *Graph
	locations          map[Location]struct{}
	realtimeBlockPairs []RealtimeBlockPair
}

func EmptyCycle(g *Graph) Cycle {
	return Cycle{
		phase:              0,
		err:                0,
		graph:              g,
		locations:          make(map[Location]struct{}),
		realtimeBlockPairs: []RealtimeBlockPair{},
	}
}

func (c *Cycle) IfAbort() bool {
	for location := range c.locations {
		txn := c.graph.GetTxn(location.tID, location.xID)
		if txn.abortByErr {
			return true
		}
	}
	return false
}

func (c *Cycle) SetErr() {
	atomic.StoreInt64(&c.err, 1)
}

func (c *Cycle) GetErr() bool {
	return atomic.LoadInt64(&c.err) >= 1
}

func (c *Cycle) SetDone() {
	atomic.StoreInt64(&c.phase, 1)
}

func (c *Cycle) GetDone() bool {
	if atomic.LoadInt64(&c.phase) >= 1 {
		return true
	}
	for location := range c.locations {
		if !c.graph.GetAction(location.tID, location.xID, location.aID).GetDone() {
			return false
		}
	}
	return true
}

func (c *Cycle) Add(location Location) {
	c.locations[location] = struct{}{}
}

func (c *Cycle) Update(oldL, newL Location) {
	delete(c.locations, oldL)
	c.locations[newL] = struct{}{}
}

func (c *Cycle) AddBlockPair(pair RealtimeBlockPair) {
	c.realtimeBlockPairs = append(c.realtimeBlockPairs, pair)
}

func (c *Cycle) String() string {
	var b strings.Builder
	b.WriteByte('[')
	i := 0
	for location := range c.locations {
		action := c.graph.GetAction(location.tID, location.xID, location.aID)
		if i != 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "[%s]", action)
		fmt.Fprintf(&b, "[%d %d %d]", action.tID, action.xID, action.id)
		i++
	}
	b.WriteByte(']')
	return b.String()
}

type RealtimeBlockPair struct {
	from Location
	to   Location
}

func RealtimeBlockPairFromActions(from, to *Action) RealtimeBlockPair {
	return RealtimeBlockPair{
		from: LocationFromAction(from),
		to:   LocationFromAction(to),
	}
}

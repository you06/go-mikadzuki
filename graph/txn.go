package graph

import (
	"fmt"
	"strings"
	"sync"
)

type Txn struct {
	sync.RWMutex
	id        int
	tID       int
	allocID   int
	actions   []Action
	status    Status
	startOuts []Depend
	startIns  []Depend
	endIns    []Depend
	endOuts   []Depend
	ifStart   bool
	ifEnd     bool
	ifReady   bool
}

func NewTxn(id, tID int, s Status) Txn {
	return Txn{
		id:        id,
		tID:       tID,
		allocID:   0,
		actions:   []Action{},
		status:    s,
		startOuts: []Depend{},
		startIns:  []Depend{},
		endIns:    []Depend{},
		endOuts:   []Depend{},
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

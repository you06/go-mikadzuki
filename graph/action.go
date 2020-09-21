package graph

import (
	"fmt"
	"strings"
	"sync"

	"github.com/you06/go-mikadzuki/kv"
)

type Action struct {
	id  int
	tID int
	tp  ActionTp
	// outs & ins are transaction dependencies,
	// which should only exist in Begin, Commit and Rollback actions
	outs []Depend
	ins  []Depend
	// vOuts & vIns are value dependencies,
	// which should only exist in DML actions
	vOuts []Depend
	vIns  []Depend
	// key id, when it's -1, it means the key is not specified yet
	kID int
	// value id, can find out value from kv.Schema
	// when the value id is -1, it means the value is None
	// missing Option generic type
	vID       int
	knowValue bool
	SQL       string
	lock      *sync.RWMutex
	ifExec    bool
	ifReady   bool
}

type ActionTp string

type DependTp string

type Depend struct {
	tID int
	aID int
	tp  DependTp
}

var (
	Begin           ActionTp = "Begin"
	Commit          ActionTp = "Commit"
	Rollback        ActionTp = "Rollback"
	Select          ActionTp = "Select"
	SelectForUpdate ActionTp = "SelectForUpdate"
	Insert          ActionTp = "Insert"
	Update          ActionTp = "Update"
	Delete          ActionTp = "Delete"
	actionTps                = []ActionTp{
		Begin,
		Commit,
		Rollback,
		Select,
		SelectForUpdate,
		Insert,
		Update,
		Delete,
	}
)

var (
	RW        DependTp = "RW"
	WW        DependTp = "WW"
	WR        DependTp = "WR"
	WRCommit  DependTp = "WRCommit"
	Realtime  DependTp = "Realtime"
	NotInit   DependTp = "NotInit"
	dependTps          = []DependTp{
		RW,
		WW,
		WR,
		Realtime,
	}
)

func NewAction(id, tID int, tp ActionTp) Action {
	return Action{
		id:        id,
		tID:       tID,
		tp:        tp,
		outs:      []Depend{},
		ins:       []Depend{},
		vOuts:     []Depend{},
		vIns:      []Depend{},
		knowValue: false,
		vID:       kv.INVALID_VALUE_ID,
		SQL:       "",
		lock:      &sync.RWMutex{},
		ifExec:    false,
		ifReady:   false,
	}
}

func (a ActionTp) IsRead() bool {
	return a == Select || a == SelectForUpdate
}

func (a ActionTp) IsWrite() bool {
	return a == Insert || a == Update || a == Delete
}

func (a ActionTp) IsTxnBegin() bool {
	return a == Begin
}

func (a ActionTp) IsTxnEnd() bool {
	return a == Commit || a == Rollback
}

func (a ActionTp) IsTxn() bool {
	return a.IsTxnBegin() || a.IsTxnEnd()
}

func (d DependTp) CheckValidFrom(tp ActionTp) bool {
	switch d {
	case RW:
		return tp.IsRead()
	case WW, WR:
		return tp.IsWrite()
	default:
		panic("unreachable")
	}
}

func (d DependTp) CheckValidLastFrom(tp ActionTp) bool {
	switch d {
	case RW:
		return true
	case WW, WR:
		if tp == Commit {
			return true
		}
		return false
	default:
		panic("unreachable")
	}
}

func (d DependTp) GetActionFrom(actions []Action) Action {
	switch d {
	case WR, WW:
		return actions[len(actions)-1]
	case RW:
		return actions[0]
	default:
		panic("unreachable")
	}
}

func (d DependTp) CheckValidTo(tp ActionTp) bool {
	switch d {
	case WR:
		return tp.IsRead()
	case RW, WW:
		return tp.IsWrite()
	default:
		panic("unreachable")
	}
}

func (d DependTp) CheckValidLastTo(tp ActionTp) bool {
	switch d {
	case WR:
		return true
	case WW, RW:
		if tp == Commit {
			return true
		}
		return false
	default:
		panic("unreachable")
	}
}

func (d DependTp) GetActionTo(actions []Action) Action {
	switch d {
	case RW, WW:
		return actions[len(actions)-1]
	case WR:
		return actions[0]
	default:
		panic("unreachable")
	}
}

func (a *Action) SetExec(b bool) {
	a.lock.Lock()
	a.ifExec = b
	a.lock.Unlock()
}

func (a *Action) GetExec() bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.ifExec
}

func (a *Action) SetReady(b bool) {
	a.lock.Lock()
	a.ifReady = b
	a.lock.Unlock()
}

func (a *Action) GetReady() bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.ifReady
}

func (a *Action) String() string {
	var b strings.Builder
	b.WriteString(string(a.tp))
	if a.tp.IsRead() || a.tp.IsWrite() {
		fmt.Fprintf(&b, "(%d, %d)", a.kID, a.vID)
		for _, d := range a.vIns {
			fmt.Fprintf(&b, "[%d, %d]", d.tID, d.aID)
		}
		for _, d := range a.vOuts {
			fmt.Fprintf(&b, "{%d, %d}", d.tID, d.aID)
		}
	} else {
		for _, d := range a.ins {
			if d.tID != a.tID {
				fmt.Fprintf(&b, "[%d, %d]", d.tID, d.aID)
			}
		}
	}
	return b.String()
}

package graph

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/you06/go-mikadzuki/kv"
)

type Action struct {
	id  int
	tID int
	xID int
	tp  ActionTp
	// outs & ins are transaction dependencies,
	// record WW dependency
	outs        []Depend
	ins         []Depend
	beforeWrite Depend
	kvNext      *Depend
	// key id, when it's -1, it means the key is not specified yet
	kID int
	// value id, can find out value from kv.Schema
	// when the value id is -1, it means the value is None
	// missing Option generic type
	vID       int
	knowValue bool
	SQL       string
	// execution phase
	// 0: not ready
	// 1: ready to execute
	// 2: executed but not returned
	// 3: value returned, exec done, can set next action to ready state
	phase int64
	// anomaly fields
	ExpectedErrorMsg string
	abortOther       bool
	abortSelf        bool
	mayAbortSelf     bool
	abortBlock       *Depend
	cycle            *Cycle
}

type ActionTp string

type DependTp string

type Status string

type Depend struct {
	tID int
	xID int
	aID int
	tp  DependTp
}

var INVALID_DEPEND = Depend{
	tID: -1,
	xID: -1,
	aID: -1,
	tp:  WW,
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
	Replace         ActionTp = "Replace"
	actionTps                = []ActionTp{
		Begin,
		Commit,
		Rollback,
		Select,
		SelectForUpdate,
		Insert,
		Update,
		Delete,
		Replace,
	}
)

// for easy usage, here we use order as depend name, eg.
// RW means Read and Write, same as Write after Read(WaR)
var (
	RW        DependTp = "RW"
	WW        DependTp = "WW"
	WR        DependTp = "WR"
	RR        DependTp = "RR"
	Realtime  DependTp = "Realtime"
	NotInit   DependTp = "NotInit"
	dependTps          = []DependTp{
		RW,
		WW,
		WR,
		Realtime,
	}
)

var (
	Committed  Status = "Committed"
	Rollbacked Status = "Rollbacked"
	Abort      Status = "Abortted"
)

func NewAction(id, tID, xID int, tp ActionTp) Action {
	return Action{
		id:           id,
		tID:          tID,
		xID:          xID,
		tp:           tp,
		outs:         []Depend{},
		ins:          []Depend{},
		beforeWrite:  INVALID_DEPEND,
		knowValue:    false,
		vID:          kv.INVALID_VALUE_ID,
		SQL:          "",
		phase:        0,
		abortOther:   false,
		abortSelf:    false,
		mayAbortSelf: false,
	}
}

func (a ActionTp) IsRead() bool {
	return a == Select || a == SelectForUpdate
}

func (a ActionTp) IsWrite() bool {
	return a == Insert || a == Update || a == Delete || a == Replace
}

func (a ActionTp) IsLock() bool {
	return a.IsWrite() || a == SelectForUpdate
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
	case RW:
		return actions[0]
	case WR, WW:
		return actions[len(actions)-1]
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

func (d DependTp) toFromBegin() bool {
	switch d {
	case RW:
		return true
	}
	return false
}

func (d DependTp) toFromEnd() bool {
	return !d.toFromBegin()
}

func (d DependTp) toToBegin() bool {
	switch d {
	case WR:
		return true
	}
	return false
}

func (d DependTp) toToEnd() bool {
	return !d.toToBegin()
}

func DependTpFromActionTps(t1, t2 ActionTp) DependTp {
	if t1.IsRead() {
		if t2.IsRead() {
			return RR
		} else if t2.IsWrite() {
			return RW
		}
	} else if t1.IsWrite() {
		if t2.IsRead() {
			return WR
		} else if t2.IsWrite() {
			return WW
		}
	}
	panic(fmt.Sprintf("unsuppert ActionTp %s, %s", t1, t2))
}

func (a *Action) SetReady() {
	atomic.StoreInt64(&a.phase, 1)
}

func (a *Action) GetReady() bool {
	return atomic.LoadInt64(&a.phase) >= 1
}

func (a *Action) SetExec() {
	atomic.StoreInt64(&a.phase, 2)
}

func (a *Action) GetExec() bool {
	return atomic.LoadInt64(&a.phase) >= 2
}

func (a *Action) SetDone() {
	atomic.StoreInt64(&a.phase, 3)
}

func (a *Action) GetDone() bool {
	return atomic.LoadInt64(&a.phase) >= 3
}

func (a *Action) String() string {
	var b strings.Builder
	if a.mayAbortSelf {
		b.WriteByte('*')
	}
	fmt.Fprintf(&b, "(%d)", a.id)
	b.WriteString(string(a.tp))
	fmt.Fprintf(&b, "(%d, %d)", a.kID, a.vID)
	if a.abortSelf {
		fmt.Fprintf(&b, "[[%d, %d, %d]]", a.abortBlock.tID, a.abortBlock.xID, a.abortBlock.aID)
	} else {
		for _, d := range a.ins {
			fmt.Fprintf(&b, "[%d, %d, %d]", d.tID, d.xID, d.aID)
		}
	}
	for _, d := range a.outs {
		if a.abortOther {
			fmt.Fprintf(&b, "{{%d, %d, %d}}", d.tID, d.xID, d.aID)
		} else {
			fmt.Fprintf(&b, "{%d, %d, %d}", d.tID, d.xID, d.aID)
		}
	}
	return b.String()
}

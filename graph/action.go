package graph

type Action struct {
	id int
	tp ActionTp
	// outs & ins are transaction dependencies,
	// which should only exist in Begin, Commit and Rollback actions
	outs []Depend
	ins  []Depend
	// vOuts & vIns are value dependencies,
	// which should only exist in DML actions
	vOuts []Depend
	vIns  []Depend
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
	Realtime  DependTp = "Realtime"
	NotInit   DependTp = "NotInit"
	dependTps          = []DependTp{
		RW,
		WW,
		WR,
		Realtime,
	}
)

func NewAction(id int, tp ActionTp) Action {
	return Action{
		id:    id,
		tp:    tp,
		outs:  []Depend{},
		ins:   []Depend{},
		vOuts: []Depend{},
		vIns:  []Depend{},
	}
}

func (d DependTp) CheckValidFrom(tp ActionTp) bool {
	switch d {
	case RW:
		switch tp {
		case Select, SelectForUpdate:
			return true
		default:
			return false
		}
	case WW, WR:
		switch tp {
		case Insert, Update, Delete:
			return true
		default:
			return false
		}
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
		switch tp {
		case Select, SelectForUpdate:
			return true
		default:
			return false
		}
	case RW, WW:
		switch tp {
		case Insert, Update, Delete:
			return true
		default:
			return false
		}
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

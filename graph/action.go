package graph

type Action struct {
	id       int
	actionTp ActionTp
	// depend should be located by 2 indexes, (timeline_id, action_id, depend_type)
	// outs: Vec<(i64, i64, DependTp)>,
	// ins: Vec<(i64, i64, DependTp)>,
	outs []Depend
	ins  []Depend
}

type ActionTp string

type DependTp string

type Depend struct {
	from int
	to   int
	tp   DependTp
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
	WW        DependTp = "WW"
	WR        DependTp = "WR"
	Realtime  DependTp = "Realtime"
	NotInit   DependTp = "NotInit"
	dependTps          = []DependTp{
		WW,
		WR,
		Realtime,
	}
)

func NewAction(id int, tp ActionTp) Action {
	return Action{
		id:       id,
		actionTp: tp,
		outs:     []Depend{},
		ins:      []Depend{},
	}
}

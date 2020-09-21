package graph

type Txn struct {
	id        int
	allocID   int
	actions   []Action
	status    Status
	startOuts []Depend
	startIns  []Depend
	endIns    []Depend
	endOuts   []Depend
}

func NewTxn(id int, s Status) Txn {
	return Txn{
		id:        id,
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
	action := NewAction(id, t.id, actionTp)
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

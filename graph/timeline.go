package graph

type Timeline struct {
	id      int
	allocID int
	actions []Action
}

func NewTimeline(id int) Timeline {
	return Timeline{
		id:      id,
		allocID: 0,
		actions: []Action{},
	}
}

func (t *Timeline) NewACtionWithTp(actionTp ActionTp) *Action {
	id := t.allocID
	action := NewAction(id, actionTp)
	if id > 0 {
		before := id - 1
		t.actions[before].outs = append(t.actions[before].outs, Depend{
			from: before,
			to:   t.allocID,
			tp:   Realtime,
		})
		action.ins = append(action.ins, Depend{
			from: t.allocID,
			to:   before,
			tp:   Realtime,
		})
	}
	t.allocID += 1
	t.actions = append(t.actions, action)
	return &t.actions[id]
}

func (t *Timeline) GetAction(n int) *Action {
	if n < t.allocID {
		return &t.actions[n]
	}
	return nil
}

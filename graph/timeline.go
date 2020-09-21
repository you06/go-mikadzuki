package graph

import (
	"strings"
)

type Timeline struct {
	id      int
	allocID int
	actions []Action
	Txns    []Txn
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
	action := NewAction(id, t.id, actionTp)
	if id > 0 {
		before := id - 1
		t.actions[before].outs = append(t.actions[before].outs, Depend{
			tID: t.id,
			aID: id,
			tp:  Realtime,
		})
		action.ins = append(action.ins, Depend{
			tID: t.id,
			aID: before,
			tp:  Realtime,
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

func (t *Timeline) String() string {
	var b strings.Builder
	for i, a := range t.actions {
		if i != 0 {
			b.WriteString(" -> ")
		}
		b.WriteString(a.String())
	}
	return b.String()
}

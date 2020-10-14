package graph

import (
	"strings"
)

type Timeline struct {
	id      int
	allocID int
	txns    []Txn
}

func NewTimeline(id int) Timeline {
	return Timeline{
		id:      id,
		allocID: 0,
		txns:    []Txn{},
	}
}

func (t *Timeline) String() string {
	var b strings.Builder
	for i, t := range t.txns {
		if i != 0 {
			b.WriteString("\n")
		}
		b.WriteString(t.String())
	}
	return b.String()
}

func (t *Timeline) NewTxnWithStatus(s Status) *Txn {
	id := t.allocID
	t.allocID += 1
	txn := NewTxn(id, t.id, s)
	t.txns = append(t.txns, txn)
	return &t.txns[id]
}

func (t *Timeline) GetTxn(n int) *Txn {
	if n < t.allocID {
		return &t.txns[n]
	}
	return nil
}

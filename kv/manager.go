package kv

import (
	"github.com/you06/go-mikadzuki/config"
)

type Manager struct {
	global  *config.Global
	allocID int
	kvs     []LinearKV
}

func NewManager(global *config.Global) Manager {
	return Manager{
		global:  global,
		allocID: 0,
		kvs:     []LinearKV{},
	}
}

func (m *Manager) Reset() {
	m.allocID = 0
	m.kvs = []LinearKV{}
}

func (m *Manager) NewLinearKV() *LinearKV {
	id := m.allocID
	linearKV := LinearKV{
		SchemaID:  id,
		Columns:   []Column{},
		Snapshots: []Snapshot{},
		Primary:   []int{},
		Unique:    [][]int{},
	}
	m.kvs = append(m.kvs, linearKV)
	return &m.kvs[id]
}

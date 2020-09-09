package kv

import (
	"github.com/you06/go-mikadzuki/config"
)

type Manager struct {
	global  *config.Global
	allocID int
	schemas []Schema
}

func NewManager(global *config.Global) Manager {
	return Manager{
		global:  global,
		allocID: 0,
		schemas: []Schema{},
	}
}

func (m *Manager) Reset() {
	m.allocID = 0
	m.schemas = []Schema{}
}

func (m *Manager) NewSchema() *Schema {
	id := m.allocID
	schema := Schema{
		SchemaID:   id,
		Columns:    []Column{},
		Primary:    []int{},
		Unique:     [][]int{},
		PrimarySet: make(map[string]struct{}),
		UniqueSet:  []map[string]struct{}{},
		AllocKID:   0,
		AllocVID:   0,
		KVs:        []KV{},
		VID2KID:    make(map[int]int),
		Data:       [][]interface{}{},
	}
	m.schemas = append(m.schemas, schema)
	return &m.schemas[id]
}

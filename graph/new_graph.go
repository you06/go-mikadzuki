package graph

import "github.com/you06/go-mikadzuki/kv"

func (g *Graph) NewKV() {
	pair := g.schema.NewKV()
	g.Next(pair)
}

// Next tries finding no cycle next dependent txn recursively
func (g *Graph) Next(pair *kv.KV) {

}

func (g *Graph) IfCycle1() bool {
	return false
}

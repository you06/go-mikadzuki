package graph

import "github.com/you06/go-mikadzuki/kv"

func (g *Graph) NewKV() {
	pair := g.schema.NewKV()
	g.Next(pair)
}

// Next tries finding no cycle next dependent txn recursively
func (g *Graph) Next(pair *kv.KV) {

}

func (g *Graph) IfCycle1(t1, x1, t2, x2 int, tp DependTp) bool {
	ts := len(g.timelines)
	visited := make([][]bool, ts)
	for i := 0; i < ts; i++ {
		length := 2 * len(g.timelines[i].txns)
		row := make([]bool, length)
		for j := 0; j < length; j++ {
			row[j] = false
		}
		visited[i] = row
	}

	getInOut := func(t, x int) ([]Depend, []Depend) {
		txn := g.GetTimeline(t).GetTxn(x / 2)
		if x%2 == 0 {
			return txn.startIns, txn.startOuts
		}
		return txn.endIns, txn.endOuts
	}

	var txn1Outs []Depend

	var dfs func(int, int, int, int) bool

	dfs = func(t1, x1 int, t2, x2 int) bool {
		if t1 == t2 && x1 <= x2 {
			return true
		}
		for _, out := range txn1Outs {
			if t1 == out.tID && x1 == out.aID {
				return true
			}
		}
		if visited[t1][x1] {
			return false
		}
		visited[t1][x1] = true
		return false
	}

	switch tp {
	case WW:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		return dfs(t2, 2*x2+1, t1, 2*x1+1)
	case WR:
		_, txn1Outs = getInOut(t1, 2*x1)
		return dfs(t2, 2*x2+1, t1, 2*x1)
	case RW:
		_, txn1Outs = getInOut(t1, 2*x1+1)
		return dfs(t2, 2*x2, t1, 2*x1+1)
	default:
		panic("unreachable")
	}
}

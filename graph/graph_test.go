package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func emptyGraph() *Graph {
	return &Graph{}
}

func TestConnectTxnAndSimpleCycle(t *testing.T) {
	var (
		graph    *Graph
		timeline *Timeline
		ok       bool
		path     [][2]int
	)
	graph = emptyGraph()
	timeline = graph.NewTimeline()
	_ = timeline.NewTxnWithStatus(Committed)
	timeline = graph.NewTimeline()
	_ = timeline.NewTxnWithStatus(Rollbacked)
	graph.ConnectTxn(0, 0, 1, 0, WR)
	require.Equal(t, graph, &Graph{
		allocID: 2,
		timelines: []Timeline{
			{
				id:      0,
				allocID: 1,
				txns: []Txn{
					{
						id:        0,
						tID:       0,
						allocID:   0,
						actions:   []Action{},
						status:    Committed,
						startOuts: []Depend{},
						startIns:  []Depend{},
						endIns:    []Depend{},
						endOuts: []Depend{
							{
								tID: 1,
								xID: 0,
								tp:  WR,
							},
						},
					},
				},
			},
			{
				id:      1,
				allocID: 1,
				txns: []Txn{
					{
						id:        0,
						tID:       1,
						allocID:   0,
						actions:   []Action{},
						status:    Rollbacked,
						startOuts: []Depend{},
						startIns: []Depend{
							{
								tID: 0,
								xID: 0,
								tp:  WR,
							},
						},
						endIns:  []Depend{},
						endOuts: []Depend{},
					},
				},
			},
		},
	})
	ok, path = graph.IfCycle(1, 0, 0, 0, WW)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{0, 1}, {1, 0}, {1, 1}})
	require.Equal(t, shortPath(path), [][2]int{{0, 0}, {1, 0}})
	ok, path = graph.IfCycle(1, 0, 0, 0, WR)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{0, 0}, {0, 1}, {1, 0}, {1, 1}})
	require.Equal(t, shortPath(path), [][2]int{{0, 0}, {1, 0}})
	ok, path = graph.IfCycle(0, 0, 1, 0, WW)
	require.False(t, ok)
	require.Nil(t, path)
	ok, path = graph.IfCycle(0, 0, 1, 0, RW)
	require.False(t, ok)
	require.Nil(t, path)
}

func TestCycle(t *testing.T) {
	var (
		graph *Graph
		ok    bool
		path  [][2]int
	)
	case1 := func() *Graph {
		graph := emptyGraph()
		var timeline *Timeline
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		_ = timeline.NewTxnWithStatus(Committed)
		return graph
	}
	graph = case1()
	graph.ConnectTxn(0, 0, 1, 0, WW)
	ok, path = graph.IfCycle(1, 1, 0, 0, WW)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{0, 1}, {1, 1}, {1, 2}, {1, 3}})
	require.Equal(t, shortPath(path), [][2]int{{0, 0}, {1, 0}, {1, 1}})
	graph = case1()
	graph.ConnectTxn(1, 1, 0, 0, WW)
	ok, path = graph.IfCycle(0, 0, 1, 0, WW)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{1, 1}, {1, 2}, {1, 3}, {0, 1}})

	case2 := func() *Graph {
		graph := emptyGraph()
		var timeline *Timeline
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		_ = timeline.NewTxnWithStatus(Committed)
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		return graph
	}
	graph = case2()
	graph.ConnectTxn(0, 0, 2, 0, WW)
	graph.ConnectTxn(2, 0, 1, 0, WR)
	ok, path = graph.IfCycle(1, 1, 0, 0, WW)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{0, 1}, {2, 1}, {1, 0}, {1, 1}, {1, 2}, {1, 3}})
	graph = case2()
	graph.ConnectTxn(0, 0, 2, 0, WW)
	graph.ConnectTxn(1, 1, 0, 0, WW)
	ok, path = graph.IfCycle(2, 0, 1, 0, WR)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {0, 1}, {2, 1}})
	graph = case2()
	graph.ConnectTxn(1, 1, 0, 0, WW)
	graph.ConnectTxn(2, 0, 1, 0, WR)
	ok, path = graph.IfCycle(0, 0, 2, 0, WW)
	require.Equal(t, path, [][2]int{{2, 1}, {1, 0}, {1, 1}, {1, 2}, {1, 3}, {0, 1}})
	require.True(t, ok)

	case3 := func() *Graph {
		graph := emptyGraph()
		var timeline *Timeline
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		timeline = graph.NewTimeline()
		_ = timeline.NewTxnWithStatus(Committed)
		return graph
	}
	graph = case3()
	graph.ConnectTxn(0, 0, 1, 0, WW)
	ok, path = graph.IfCycle(1, 0, 0, 0, WW)
	require.True(t, ok)
	require.Equal(t, path, [][2]int{{0, 1}, {1, 1}})
}

func TestCanDeadlock(t *testing.T) {
	require.False(t, canDeadlock([][2]int{{2, 1}, {1, 0}, {1, 1}, {1, 2}, {1, 3}, {0, 1}}))
}

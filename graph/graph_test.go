package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/you06/go-mikadzuki/kv"
)

func emptyGraph() *Graph {
	manager := kv.NewManager(nil)
	g := NewGraph(&manager)
	return &g
}

func TestCreateTimeline(t *testing.T) {
	graph := emptyGraph()
	timeline := graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Rollback)
	require.Equal(t, graph.allocID, 1)
	require.Nil(t, graph.GetTimeline(1))
	require.Nil(t, graph.GetTimeline(2))
	require.Equal(t, graph.GetTimeline(0).allocID, 3)
	require.NotNil(t, graph.GetTimeline(0).GetAction(2))
	require.Nil(t, graph.GetTimeline(0).GetAction(3))
}

func TestIfPossible(t *testing.T) {
	graph := emptyGraph()
	timeline := graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Insert) // 1
	timeline.NewACtionWithTp(Commit) // 2
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Select) // 1
	timeline.NewACtionWithTp(Update) // 2
	timeline.NewACtionWithTp(Commit) // 3
	timeline.NewACtionWithTp(Begin)  // 4
	timeline.NewACtionWithTp(Select) // 5
	timeline.NewACtionWithTp(Commit) // 6
	timeline.NewACtionWithTp(Begin)  // 7
	timeline.NewACtionWithTp(Update) // 8
	timeline.NewACtionWithTp(Commit) // 9
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Select) // 1
	timeline.NewACtionWithTp(Update) // 2
	timeline.NewACtionWithTp(Commit) // 3
	timeline.NewACtionWithTp(Begin)  // 4
	timeline.NewACtionWithTp(Select) // 5
	timeline.NewACtionWithTp(Commit) // 6
	graph.ConnectValueDepend(0, 1, 1, 5, WR)
	require.False(t, graph.IfPossible(0, 1, 1, 2, WW))
	require.True(t, graph.IfPossible(0, 1, 1, 8, WW))
	graph.ConnectValueDepend(0, 1, 2, 2, WW)
	require.False(t, graph.IfPossible(0, 1, 2, 5, WR))
	require.True(t, graph.IfPossible(0, 1, 2, 1, WR))
}

func TestIfCycle(t *testing.T) {
	graph := emptyGraph()
	timeline := graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Insert) // 1
	timeline.NewACtionWithTp(Commit) // 2
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Insert) // 1
	timeline.NewACtionWithTp(Commit) // 2
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Insert) // 1
	timeline.NewACtionWithTp(Commit) // 2
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)  // 0
	timeline.NewACtionWithTp(Insert) // 1
	timeline.NewACtionWithTp(Commit) // 2
	graph.ConnectTxnDepend(0, 2, 1, 0, WW)
	require.True(t, graph.IfCycle(1, 2, 0, 1))
	graph.ConnectTxnDepend(1, 2, 2, 0, WW)
	graph.ConnectTxnDepend(2, 2, 3, 0, WW)
	require.True(t, graph.IfCycle(3, 2, 0, 0))
}

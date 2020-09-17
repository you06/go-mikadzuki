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
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Insert)
	timeline.NewACtionWithTp(Commit)
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Select)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	timeline = graph.NewTimeline()
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Select)
	timeline.NewACtionWithTp(Commit)
	timeline.NewACtionWithTp(Begin)
	timeline.NewACtionWithTp(Update)
	timeline.NewACtionWithTp(Commit)
	graph.ConnectValueDepend(1, 1, 0, 1, WW)
	graph.ConnectValueDepend(0, 1, 1, 7, WR)
	require.False(t, graph.IfPossible(0, 1, 1, 4, WW))
	require.True(t, graph.IfPossible(0, 1, 1, 10, WW))
	graph.ConnectValueDepend(0, 1, 2, 4, WW)
	graph.ConnectValueDepend(1, 10, 2, 7, WR)
	require.False(t, graph.IfPossible(0, 1, 1, 10, WW))
	require.True(t, graph.IfPossible(2, 4, 1, 10, WW))
}

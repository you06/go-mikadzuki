package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/you06/go-mikadzuki/kv"
)

func TestCreateTimeline(t *testing.T) {
	manager := kv.NewManager(nil)
	graph := NewGraph(&manager)
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

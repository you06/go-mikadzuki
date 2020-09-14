package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := NewConfig()
	// global fields
	require.Equal(t, config.Global.DSN, "root:@tcp(172.17.0.1:4000)/")
	require.Equal(t, config.Global.Database, "mikadzuki")
	require.Equal(t, config.Global.Target, "mysql")
	require.Equal(t, config.Global.Thread, 8)
	require.Equal(t, config.Global.Action, 20)
	require.Equal(t, config.Global.LogPath, "")
	// graph fields
	require.Equal(t, config.Graph.Begin, 20)
	require.Equal(t, config.Graph.Commit, 20)
	require.Equal(t, config.Graph.Rollback, 20)
	require.Equal(t, config.Graph.Select, 30)
	require.Equal(t, config.Graph.SelectForUpdate, 30)
	require.Equal(t, config.Graph.Insert, 50)
	require.Equal(t, config.Graph.Update, 50)
	require.Equal(t, config.Graph.Delete, 50)
	// depend fields
	require.Equal(t, config.Depend.WW, 10)
	require.Equal(t, config.Depend.WR, 10)
	require.Equal(t, config.Depend.RW, 10)
}

func TestLoadConfig(t *testing.T) {
	config := NewConfig()
	require.Nil(t, config.Load("./config.test.toml"))
	// global fields
	require.Equal(t, config.Global.DSN, "root:@tcp(172.17.0.1:3306)/")
	require.Equal(t, config.Global.Database, "test")
	require.Equal(t, config.Global.Target, "tidb")
	require.Equal(t, config.Global.Thread, 4)
	require.Equal(t, config.Global.Action, 10)
	require.Equal(t, config.Global.LogPath, "./logs")
	// graph fields
	require.Equal(t, config.Graph.Begin, 2)
	require.Equal(t, config.Graph.Commit, 2)
	require.Equal(t, config.Graph.Rollback, 2)
	require.Equal(t, config.Graph.Select, 3)
	require.Equal(t, config.Graph.SelectForUpdate, 3)
	require.Equal(t, config.Graph.Insert, 5)
	require.Equal(t, config.Graph.Update, 5)
	require.Equal(t, config.Graph.Delete, 5)
	// depend fields
	require.Equal(t, config.Depend.WW, 1)
	require.Equal(t, config.Depend.WR, 1)
	// test ToMap of graph config
	graphMap := config.Graph.ToMap()
	require.Equal(t, graphMap, map[string]int{
		"Begin":           2,
		"Commit":          2,
		"Rollback":        2,
		"Select":          3,
		"SelectForUpdate": 3,
		"Insert":          5,
		"Update":          5,
		"Delete":          5,
	})
	// test ToMap of depend config
	dependMap := config.Depend.ToMap()
	require.Equal(t, dependMap, map[string]int{
		"WW": 1,
		"WR": 1,
		"RW": 1,
	})
}

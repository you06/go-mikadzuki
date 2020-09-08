package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDependCheckFunctions(t *testing.T) {
	// from
	// RW
	require.True(t, RW.CheckValidFrom(Select))
	require.True(t, RW.CheckValidFrom(SelectForUpdate))
	require.False(t, RW.CheckValidFrom(Insert))
	require.False(t, RW.CheckValidFrom(Update))
	require.False(t, RW.CheckValidFrom(Delete))
	require.True(t, RW.CheckValidLastFrom(Commit))
	require.True(t, RW.CheckValidLastFrom(Rollback))
	// WW
	require.False(t, WW.CheckValidFrom(Select))
	require.False(t, WW.CheckValidFrom(SelectForUpdate))
	require.True(t, WW.CheckValidFrom(Insert))
	require.True(t, WW.CheckValidFrom(Update))
	require.True(t, WW.CheckValidFrom(Delete))
	require.True(t, WW.CheckValidLastFrom(Commit))
	require.False(t, WW.CheckValidLastFrom(Rollback))
	// WR
	require.False(t, WR.CheckValidFrom(Select))
	require.False(t, WR.CheckValidFrom(SelectForUpdate))
	require.True(t, WR.CheckValidFrom(Insert))
	require.True(t, WR.CheckValidFrom(Update))
	require.True(t, WR.CheckValidFrom(Delete))
	require.True(t, WR.CheckValidLastFrom(Commit))
	require.False(t, WR.CheckValidLastFrom(Rollback))
	// to
	// RW
	require.False(t, RW.CheckValidTo(Select))
	require.False(t, RW.CheckValidTo(SelectForUpdate))
	require.True(t, RW.CheckValidTo(Insert))
	require.True(t, RW.CheckValidTo(Update))
	require.True(t, RW.CheckValidTo(Delete))
	require.True(t, RW.CheckValidLastTo(Commit))
	require.False(t, RW.CheckValidLastTo(Rollback))
	// WW
	require.False(t, WW.CheckValidTo(Select))
	require.False(t, WW.CheckValidTo(SelectForUpdate))
	require.True(t, WW.CheckValidTo(Insert))
	require.True(t, WW.CheckValidTo(Update))
	require.True(t, WW.CheckValidLastTo(Commit))
	require.False(t, WW.CheckValidLastTo(Rollback))
	require.True(t, WW.CheckValidTo(Delete))
	// WR
	require.True(t, WR.CheckValidTo(Select))
	require.True(t, WR.CheckValidTo(SelectForUpdate))
	require.False(t, WR.CheckValidTo(Insert))
	require.False(t, WR.CheckValidTo(Update))
	require.False(t, WR.CheckValidTo(Delete))
	require.True(t, WR.CheckValidLastTo(Commit))
	require.True(t, WR.CheckValidLastTo(Rollback))
}

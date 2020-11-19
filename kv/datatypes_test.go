package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDatetime(t *testing.T) {
	ti, err := time.Parse(DATETIME_FORMAT, "2011-04-05 14:19:19")
	require.Nil(t, err)
	require.Equal(t, Date.ToHashString(ti), "2011-04-05")
	require.Equal(t, Date.ValToString(ti), `"2011-04-05"`)
	require.Equal(t, Date.ValToPureString(ti), "2011-04-05")
	for _, i := range []DataType{Datetime, Timestamp} {
		require.Equal(t, i.ToHashString(ti), "2011-04-05 14:19:19")
		require.Equal(t, i.ValToString(ti), `"2011-04-05 14:19:19"`)
		require.Equal(t, i.ValToPureString(ti), "2011-04-05 14:19:19")
	}
}

func TestInt(t *testing.T) {
	num := 1926
	for _, i := range []DataType{TinyInt, Int, BigInt} {
		require.Equal(t, i.ToHashString(num), "1926")
		require.Equal(t, i.ValToString(num), "1926")
		require.Equal(t, i.ValToPureString(num), "1926")
	}
}

func TestString(t *testing.T) {
	s := "0817"
	for _, i := range []DataType{Char, Varchar, Text} {
		require.Equal(t, i.ToHashString(s), "0817")
		require.Equal(t, i.ValToString(s), `"0817"`)
		require.Equal(t, i.ValToPureString(s), "0817")
	}
}

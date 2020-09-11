package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	schema = Schema{
		SchemaID: 1,
		Columns: []Column{
			{
				Name:    "id",
				Tp:      Int,
				Size:    11,
				Null:    false,
				Primary: true,
			},
			{
				Name:    "val",
				Tp:      Varchar,
				Size:    255,
				Null:    false,
				Primary: true,
			},
			{
				Name:    "k",
				Tp:      Date,
				Size:    0,
				Null:    true,
				Primary: true,
			},
		},
		Primary: []int{0, 1},
		Unique: [][]int{
			{1, 2},
		},
		PrimarySet: map[string]struct{}{
			"17-kaeru": {},
		},
		UniqueSet: []map[string]struct{}{{
			"kaeru-2020-08-31": {},
		}},
		AllocKID: 0,
		AllocVID: 0,
		VID2KID:  map[int]int{},
		KVs:      []KV{},
		Data: [][]interface{}{
			{17, "kaeru", "2020-08-31"},
		},
	}
)

func TestSchema(t *testing.T) {
	require.Equal(t, schema.CreateTable(), `CREATE TABLE t1(
id INT(11) NOT NULL,
val VARCHAR(255) NOT NULL,
k DATE NULL,
PRIMARY KEY(id, val),
UNIQUE u_0(val, k))`)
}

func TestIfKeyDuplicated(t *testing.T) {
	primaryKey := make([]string, 2)
	uniqueKeys := make([][]string, 1)
	var value []interface{}
	value = []interface{}{17, "kaeru", "2020-08-31"}
	schema.MakePrimaryKey(value, &primaryKey)
	schema.MakeUniqueKey(value, &uniqueKeys)
	require.True(t, schema.IfKeyDuplicated(value, &primaryKey, &uniqueKeys))
	value = []interface{}{17, "kaeru", "2020-08-17"}
	schema.MakePrimaryKey(value, &primaryKey)
	schema.MakeUniqueKey(value, &uniqueKeys)
	require.True(t, schema.IfKeyDuplicated(value, &primaryKey, &uniqueKeys))
	value = []interface{}{10, "kaeru", "2020-08-31"}
	schema.MakePrimaryKey(value, &primaryKey)
	schema.MakeUniqueKey(value, &uniqueKeys)
	require.True(t, schema.IfKeyDuplicated(value, &primaryKey, &uniqueKeys))
	value = []interface{}{10, "kaeru", "2020-08-17"}
	schema.MakePrimaryKey(value, &primaryKey)
	schema.MakeUniqueKey(value, &uniqueKeys)
	require.False(t, schema.IfKeyDuplicated(value, &primaryKey, &uniqueKeys))
	schema.AddPrimaryKey(primaryKey)
	require.True(t, schema.IfKeyDuplicated(value, &primaryKey, &uniqueKeys))
}

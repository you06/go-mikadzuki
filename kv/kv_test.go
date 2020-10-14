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
		AllocKID: 1,
		AllocVID: 2,
		VID2KID:  map[int]int{},
		KVs: []KV{
			{
				ID: 0,
			},
		},
		Data: [][]interface{}{
			{17, "kaeru", "2020-08-31"},
			{18, "kaeru", "1919-08-10"},
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

func TestSelectSQL(t *testing.T) {
	selectSQL := schema.SelectSQL(0)
	require.True(t, selectSQL == `SELECT * FROM t1 WHERE id=17 AND val="kaeru"` ||
		selectSQL == `SELECT * FROM t1 WHERE val="kaeru" AND k="2020-08-31"`)
}

func TestUpdateSQL(t *testing.T) {
	updateSQL := schema.UpdateSQL(0, 1)
	require.True(t, updateSQL == `UPDATE t1 SET id=18, k="1919-08-10" WHERE id=17 AND val="kaeru"` ||
		updateSQL == `UPDATE t1 SET id=18, k="1919-08-10" WHERE val="kaeru" AND k="2020-08-31"`)
}

func TestDeleteSQL(t *testing.T) {
	deleteSQL := schema.DeleteSQL(0)
	require.True(t, deleteSQL == `DELETE FROM t1 WHERE id=17 AND val="kaeru"` ||
		deleteSQL == `DELETE FROM t1 WHERE val="kaeru" AND k="2020-08-31"`)
}

func TestInsertSQL(t *testing.T) {
	insertSQL := schema.InsertSQL(1)
	require.Equal(t, insertSQL, `INSERT INTO t1 VALUES(18, "kaeru", "1919-08-10")`)
}

func TestReplace(t *testing.T) {
	newID := schema.RepValue(0, 0)
	require.Equal(t, schema.Data[newID][0], 17)
	require.Equal(t, schema.Data[newID][1], "kaeru")
}

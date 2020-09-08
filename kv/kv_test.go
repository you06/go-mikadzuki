package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchema(t *testing.T) {
	linearKV := LinearKV{
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
		Snapshots: []Snapshot{},
		Primary:   []int{0, 1},
		Unique: [][]int{
			{1, 2},
		},
	}
	require.Equal(t, linearKV.Schema(), `CREATE TABLE t1(
id INT(11) NOT NULL,
val VARCHAR(255) NOT NULL,
k DATE NULL,
PRIMARY KEY(id, val),
UNIQUE u_0(val, k))`)
}

package kv

import (
	"fmt"
	"strings"
)

type LinearKV struct {
	SchemaID  int
	Columns   []Column
	Snapshots []Snapshot
	Primary   []int
	Unique    [][]int
}

type Snapshot struct {
	Data []interface{}
}

type Column struct {
	Name    string
	Tp      DataType
	Size    int
	Null    bool
	Primary bool
}

func (l *LinearKV) Schema() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE t%d(\n", l.SchemaID)
	for i, column := range l.Columns {
		if column.Size > 0 {
			fmt.Fprintf(&b, "%s %s(%d)", column.Name, column.Tp, column.Size)
		} else {
			fmt.Fprintf(&b, "%s %s", column.Name, column.Tp)
		}
		if !column.Null {
			b.WriteString(" NOT")
		}
		b.WriteString(" NULL")
		if i != len(l.Columns)-1 {
			b.WriteString(",\n")
		}
	}

	var indexes []string
	ps := len(l.Primary)
	if ps > 0 {
		columns := make([]string, ps)
		for i := 0; i < ps; i++ {
			columns[i] = l.Columns[l.Primary[i]].Name
		}
		indexes = append(indexes, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(columns, ", ")))
	}

	for i, unique := range l.Unique {
		us := len(unique)
		columns := make([]string, us)
		for j := 0; j < us; j++ {
			columns[j] = l.Columns[unique[j]].Name
		}
		indexes = append(indexes, fmt.Sprintf("UNIQUE u_%d(%s)", i, strings.Join(columns, ", ")))
	}

	for _, index := range indexes {
		fmt.Fprintf(&b, ",\n%s", index)
	}

	b.WriteString(")")
	return b.String()
}

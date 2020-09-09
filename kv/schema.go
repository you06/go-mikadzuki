package kv

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/you06/go-mikadzuki/util"
)

// TODO: move this into config file
const UNIQUE_RATIO = 0.3

type Schema struct {
	SchemaID   int
	Columns    []Column
	Primary    []int
	Unique     [][]int
	PrimarySet map[string]struct{}
	UniqueSet  []map[string]struct{}
	AllocKID   int
	AllocVID   int
	VID2KID    map[int]int
	KVs        []KV
	Data       [][]interface{}
}

type Column struct {
	Name    string
	Tp      DataType
	Size    int
	Null    bool
	Primary bool
}

func (s *Schema) AddColumn() {
	tp := RdType()
	null := util.RdBool()
	primary := false
	if null {
		primary = util.RdBool()
		if primary {
			s.Primary = append(s.Primary, len(s.Columns))
		}
	}
	column := Column{
		Name:    fmt.Sprintf("col_%d", len(s.Columns)),
		Tp:      tp,
		Size:    tp.Size(),
		Null:    null,
		Primary: primary,
	}
	s.Columns = append(s.Columns, column)
}

func (s *Schema) AddUnique() {
	var unique []int
	for i := 0; i < len(s.Columns); i++ {
		if rand.Float64() < UNIQUE_RATIO {
			unique = append(unique, i)
		}
	}
	if len(unique) == 0 {
		unique = append(unique, rand.Intn(len(s.Columns)))
	}
	s.Unique = append(s.Unique, unique)
	s.UniqueSet = append(s.UniqueSet, make(map[string]struct{}))
}

func (s *Schema) TableName() string {
	return fmt.Sprintf("t%d", s.SchemaID)
}

func (s *Schema) CreateTable() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s(\n", s.TableName())
	for i, column := range s.Columns {
		if column.Size > 0 {
			fmt.Fprintf(&b, "%s %s(%d)", column.Name, column.Tp, column.Size)
		} else {
			fmt.Fprintf(&b, "%s %s", column.Name, column.Tp)
		}
		if !column.Null {
			b.WriteString(" NOT")
		}
		b.WriteString(" NULL")
		if i != len(s.Columns)-1 {
			b.WriteString(",\n")
		}
	}

	var indexes []string
	ps := len(s.Primary)
	if ps > 0 {
		columns := make([]string, ps)
		for i := 0; i < ps; i++ {
			columns[i] = s.Columns[s.Primary[i]].Name
		}
		indexes = append(indexes, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(columns, ", ")))
	}

	for i, unique := range s.Unique {
		us := len(unique)
		columns := make([]string, us)
		for j := 0; j < us; j++ {
			columns[j] = s.Columns[unique[j]].Name
		}
		indexes = append(indexes, fmt.Sprintf("UNIQUE u_%d(%s)", i, strings.Join(columns, ", ")))
	}

	for _, index := range indexes {
		fmt.Fprintf(&b, ",\n%s", index)
	}

	b.WriteString(")")
	return b.String()
}

func (s *Schema) NewKV() *KV {
	id := s.AllocKID
	kv := NewKV(id)
	s.AllocKID += 1
	s.KVs = append(s.KVs, kv)
	return &s.KVs[id]
}

func (s *Schema) NewValue(kID int) int {
	id := s.AllocVID
	s.AllocVID += 1
	s.VID2KID[id] = kID
	s.CreateValue(id)
	return id
}

func (s *Schema) PutValue(kID int) int {
	id := s.AllocVID
	s.AllocVID += 1
	s.VID2KID[id] = kID
	s.UpdateValue(id)
	return id
}

func (s *Schema) IfKeyDuplicated(value []interface{}, primaryKey *[]string, uniqueKeys *[][]string) bool {
	if len(s.Primary) > 0 {
		for i := 0; i < len(s.Primary); i++ {
			pos := s.Primary[i]
			(*primaryKey)[i] = s.Columns[pos].Tp.ToHashString(value[pos])
		}
		// if this value cause primary key duplicated, retry it
		if _, ok := s.PrimarySet[strings.Join((*primaryKey), "-")]; ok {
			return true
		}
	}
	for i := 0; i < len(s.Unique); i++ {
		uniqueKey := make([]string, len(s.Unique[i]))
		for j := 0; j < len(s.Unique[i]); j++ {
			pos := s.Unique[i][j]
			uniqueKey[j] = s.Columns[pos].Tp.ToHashString(value[pos])
		}
		if _, ok := s.UniqueSet[i][strings.Join(uniqueKey, "-")]; ok {
			return true
		}
		(*uniqueKeys)[i] = uniqueKey
	}
	return false
}

// TODO: add test for it
func (s *Schema) CreateValue(vID int) {
	if len(s.Data) != vID {
		panic("data and value index mismatch")
	}
	cols := len(s.Columns)
	value := make([]interface{}, cols)
	primaryKey := make([]string, len(s.Primary))
	uniqueKeys := make([][]string, len(s.Unique))
	for {
		for i := 0; i < cols; i++ {
			value[i] = s.Columns[i].Tp.RandValue()
		}
		dup := s.IfKeyDuplicated(value, &primaryKey, &uniqueKeys)
		if dup {
			continue
		}
		s.PrimarySet[strings.Join(primaryKey, "-")] = struct{}{}
		for i := 0; i < len(uniqueKeys); i++ {
			s.UniqueSet[i][strings.Join((uniqueKeys)[i], "-")] = struct{}{}
		}
		break
	}
	// check if index valid
	s.Data = append(s.Data, value)
}

func (s *Schema) UpdateValue(vID int) {

}

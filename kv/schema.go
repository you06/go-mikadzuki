package kv

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/you06/go-mikadzuki/util"
)

// TODO: move these constants into config file
const (
	COLUMN_NUM    = 10
	INDEX_NUM     = 2
	PRIMARY_RATIO = 0.2
	UNIQUE_RATIO  = 0.3
)

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

func (s *Schema) AddColumn(mustPrimary bool) {
	tp := RdType()
	null := mustPrimary || util.RdBool()
	primary := mustPrimary || false
	if null && !mustPrimary {
		primary = util.RdBoolRatio(PRIMARY_RATIO)
	}
	if primary {
		s.Primary = append(s.Primary, len(s.Columns))
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

// NewKV only declear the key but the value may be none
// when the value is none, read it will get empty value
func (s *Schema) NewKV() *KV {
	id := s.AllocKID
	kv := NewKV(id)
	s.AllocKID += 1
	s.KVs = append(s.KVs, kv)
	return &s.KVs[id]
}

// NewValue create value for a given key (Insert operation)
func (s *Schema) NewValue(kID int) int {
	id := s.AllocVID
	s.AllocVID += 1
	s.VID2KID[id] = kID
	s.CreateValue(id)
	return id
}

// PutValue update value for a given key (Update operation)
func (s *Schema) PutValue(kID, oldID int) int {
	newID := s.AllocVID
	s.AllocVID += 1
	s.VID2KID[newID] = kID
	s.UpdateValue(oldID, newID)
	return newID
}

// DelValue delete value for a given key (Delete operation)
func (s *Schema) DelValue(vID int) {
	s.DeleteValue(vID)
}

func (s *Schema) IfKeyDuplicated(value []interface{}, primaryKey *[]string, uniqueKeys *[][]string) bool {
	// if this value cause primary key duplicated, retry it
	if _, ok := s.PrimarySet[strings.Join((*primaryKey), "-")]; ok {
		return true
	}
	for i := 0; i < len(s.Unique); i++ {
		if _, ok := s.UniqueSet[i][strings.Join((*uniqueKeys)[i], "-")]; ok {
			return true
		}
	}
	return false
}

func (s *Schema) AddPrimaryKey(primaryKey []string) {
	s.PrimarySet[strings.Join((primaryKey), "-")] = struct{}{}
}

func (s *Schema) DelPrimaryKey(primaryKey []string) {
	delete(s.PrimarySet, strings.Join((primaryKey), "-"))
}

func (s *Schema) AddUniqueKeys(uniqueKeys [][]string) {
	for i := 0; i < len(s.Unique); i++ {
		s.UniqueSet[i][strings.Join(uniqueKeys[i], "-")] = struct{}{}
	}
}

func (s *Schema) DelUniqueKeys(uniqueKeys [][]string) {
	for i := 0; i < len(s.Unique); i++ {
		delete(s.UniqueSet[i], strings.Join(uniqueKeys[i], "-"))
	}
}

func (s *Schema) MakePrimaryKey(value []interface{}, primaryKey *[]string) {
	for i := 0; i < len(s.Primary); i++ {
		pos := s.Primary[i]
		(*primaryKey)[i] = s.Columns[pos].Tp.ToHashString(value[pos])
	}
}

func (s *Schema) MakeUniqueKey(value []interface{}, uniqueKeys *[][]string) {
	for i := 0; i < len(s.Unique); i++ {
		uniqueKey := make([]string, len(s.Unique[i]))
		for j := 0; j < len(s.Unique[i]); j++ {
			pos := s.Unique[i][j]
			uniqueKey[j] = s.Columns[pos].Tp.ToHashString(value[pos])
		}
		(*uniqueKeys)[i] = uniqueKey
	}
}

func (s *Schema) MakeValue() []interface{} {
	cols := len(s.Columns)
	value := make([]interface{}, cols)
	for i := 0; i < cols; i++ {
		value[i] = s.Columns[i].Tp.RandValue()
	}
	return value
}

// TODO: add test for it
func (s *Schema) CreateValue(vID int) {
	if len(s.Data) != vID {
		panic("data and value index mismatch")
	}
	var value []interface{}
	primaryKey := make([]string, len(s.Primary))
	uniqueKeys := make([][]string, len(s.Unique))

	for {
		value = s.MakeValue()
		s.MakePrimaryKey(value, &primaryKey)
		s.MakeUniqueKey(value, &uniqueKeys)
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

// there can be difference when updating value
// 1. update a non-index column is easy, it won't make any changes
// 2. update a unique-index would may cause some complex problem
// if a WW dependency is caused by a replace into, this kv will split
// 3. update primary key is similar to unique-index, but it may cause further influence
// For the above, the key ID in graph should not be changed by this update
func (s *Schema) UpdateValue(oldID, newID int) {
	if len(s.Data) != newID {
		panic("data and value index mismatch")
	}
	var value []interface{}
	primaryKey := make([]string, len(s.Primary))
	uniqueKeys := make([][]string, len(s.Unique))
	// delete old index, so that it won't conflict with itself
	if oldID != NULL_VALUE_ID {
		s.MakePrimaryKey(s.Data[oldID], &primaryKey)
		s.MakeUniqueKey(s.Data[oldID], &uniqueKeys)
		s.DelPrimaryKey(primaryKey)
		s.DelUniqueKeys(uniqueKeys)
	}
	for {
		value = s.MakeValue()
		s.MakePrimaryKey(value, &primaryKey)
		s.MakeUniqueKey(value, &uniqueKeys)
		dup := s.IfKeyDuplicated(value, &primaryKey, &uniqueKeys)
		if dup {
			continue
		}
		// add new index
		s.AddPrimaryKey(primaryKey)
		s.AddUniqueKeys(uniqueKeys)
		break
	}
	// check if index valid
	s.Data = append(s.Data, value)
}

func (s *Schema) DeleteValue(vID int) {
	if vID == NULL_VALUE_ID {
		return
	}
	primaryKey := make([]string, len(s.Primary))
	uniqueKeys := make([][]string, len(s.Unique))
	s.MakePrimaryKey(s.Data[vID], &primaryKey)
	s.MakeUniqueKey(s.Data[vID], &uniqueKeys)
	s.DelPrimaryKey(primaryKey)
	s.DelUniqueKeys(uniqueKeys)
}

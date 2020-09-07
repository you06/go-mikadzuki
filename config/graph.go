package config

import (
	"reflect"
)

type Graph struct {
	Begin           int `toml:"begin"`
	Commit          int `toml:"commit"`
	Rollback        int `toml:"rollback"`
	Select          int `toml:"select"`
	SelectForUpdate int `toml:"select-for-update"`
	Insert          int `toml:"insert"`
	Update          int `toml:"update"`
	Delete          int `toml:"delete"`
}

func NewGraph() Graph {
	return Graph{
		Begin:           20,
		Commit:          20,
		Rollback:        20,
		Select:          30,
		SelectForUpdate: 30,
		Insert:          50,
		Update:          50,
		Delete:          50,
	}
}

func (g *Graph) ToMap() map[string]int {
	val := reflect.ValueOf(g).Elem()
	fields := val.NumField()
	m := make(map[string]int, fields)

	for i := 0; i < fields; i++ {
		valueField, typeField := val.Field(i), val.Type().Field(i)
		m[typeField.Name] = valueField.Interface().(int)
	}
	return m
}

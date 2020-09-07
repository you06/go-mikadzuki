package config

import "reflect"

type Depend struct {
	WW int `toml:"ww"`
	WR int `toml:"wr"`
}

func NewDepend() Depend {
	return Depend{
		WW: 10,
		WR: 10,
	}
}

func (d *Depend) ToMap() map[string]int {
	val := reflect.ValueOf(d).Elem()
	fields := val.NumField()
	m := make(map[string]int, fields)

	for i := 0; i < fields; i++ {
		valueField, typeField := val.Field(i), val.Type().Field(i)
		m[typeField.Name] = valueField.Interface().(int)
	}
	return m
}

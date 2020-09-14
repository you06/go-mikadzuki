package kv

import (
	"database/sql"

	"github.com/juju/errors"
)

// QueryItem define query result
type QueryItem struct {
	Null      bool
	ValType   *sql.ColumnType
	ValString string
}

func ParseFromSQLResult(rows *sql.Rows) ([][]*QueryItem, error) {
	columnTypes, _ := rows.ColumnTypes()
	var result [][]*QueryItem

	for rows.Next() {
		var (
			rowResultSets []interface{}
			resultRow     []*QueryItem
		)
		for range columnTypes {
			rowResultSets = append(rowResultSets, new(interface{}))
		}
		if err := rows.Scan(rowResultSets...); err != nil {
			return nil, errors.Trace(err)
		}
		for index, resultItem := range rowResultSets {
			r := *resultItem.(*interface{})
			item := QueryItem{
				ValType: columnTypes[index],
			}
			if r != nil {
				bytes := r.([]byte)
				item.ValString = string(bytes)
			} else {
				item.Null = true
			}
			resultRow = append(resultRow, &item)
		}
		result = append(result, resultRow)
	}
	return result, nil
}

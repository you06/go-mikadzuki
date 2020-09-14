package kv

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/you06/go-mikadzuki/util"
)

type DataType int

type Null struct{}

const (
	TinyInt DataType = iota
	// SmallInt
	// MediumInt
	Int
	BigInt
	// Decimal
	// Numeric
	// Float
	// Double
	// Bit
	Date
	Datetime
	Timestamp
	// Year
	Char
	Varchar
	// Binary
	// Blob
	// TinyText
	Text
	// MediumText
	// LongText
	// Enum
	// Set
	// JSON
)

func RdType() DataType {
	return DataType(rand.Intn(int(Text)))
}

func (d DataType) String() string {
	switch d {
	case TinyInt:
		return "TINYINT"
	// case SmallInt:
	// 	return "SMALLINT"
	// case MediumInt:
	// 	return "MEDIUMINT"
	case Int:
		return "INT"
	case BigInt:
		return "BIGINT"
	case Date:
		return "DATE"
	case Datetime:
		return "DATETIME"
	case Timestamp:
		return "TIMESTAMP"
	case Char:
		return "CHAR"
	case Varchar:
		return "VARCHAR"
	// case TinyText:
	// 	return "TINYTEXT"
	case Text:
		return "TEXT"
	// case MediumText:
	// 	return "MEDIUMTEXT"
	// case LongText:
	// 	return "LONGTEXT"
	default:
		return "UNKNOWN"
	}
}

func (d DataType) RandValue() interface{} {
	switch d {
	case TinyInt:
		return util.RdRange(-128, 127)
	// case SmallInt:
	// 	return "SMALLINT"
	// case MediumInt:
	// 	return "MEDIUMINT"
	case Int:
		return util.RdRange(-2147483648, 2147483647)
	case BigInt:
		return util.RdRange(-9223372036854775808, 9223372036854775807)
	case Date:
		return util.RdDate()
	case Datetime:
		return util.RdDateTime()
	case Timestamp:
		return util.RdTimestamp()
	case Char:
		return util.RdName()
	case Varchar:
		return util.RdName()
	// case TinyText:
	// 	return "TINYTEXT"
	case Text:
		return util.RdName()
	default:
		panic(fmt.Sprintf("unimplement type %s", d))
	}
}

// use default size by now
func (d DataType) Size() int {
	switch d {
	case Varchar:
		return util.RdRange(127, 511)
	case Char:
		return util.RdRange(31, 255)
	}
	return 0
}

func (d DataType) ToHashString(data interface{}) string {
	// null value will not lead to duplicated unique key
	// here we use random hash string to avoid it
	if _, ok := data.(Null); ok {
		return util.RdHash()
	}
	switch d {
	case TinyInt, Int, BigInt:
		return strconv.Itoa(data.(int))
	case Date, Datetime, Timestamp, Char, Varchar, Text:
		return data.(string)
	default:
		panic(fmt.Sprintf("unimplement type %s", d))
	}
}

func (d DataType) ValToString(data interface{}) string {
	// null value will not lead to duplicated unique key
	// here we use random hash string to avoid it
	if _, ok := data.(Null); ok {
		return "NULL"
	}
	switch d {
	case TinyInt, Int, BigInt:
		return strconv.Itoa(data.(int))
	case Date, Datetime, Timestamp, Char, Varchar, Text:
		return fmt.Sprintf(`"%s"`, data.(string))
	default:
		panic(fmt.Sprintf("unimplement type %s", d))
	}
}

func (d DataType) ValToPureString(data interface{}) string {
	// null value will not lead to duplicated unique key
	// here we use random hash string to avoid it
	if _, ok := data.(Null); ok {
		return "NULL"
	}
	switch d {
	case TinyInt, Int, BigInt:
		return strconv.Itoa(data.(int))
	case Date, Datetime, Timestamp, Char, Varchar, Text:
		return data.(string)
	default:
		panic(fmt.Sprintf("unimplement type %s", d))
	}
}

package kv

type DataType int

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
	}
	return "UNKNOWN"
}

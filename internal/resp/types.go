package resp

import "errors"

// RESP Type byte prefixes
const (
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	IntegerPrefix      = ':'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
)

// Common RESP errors
var (
	ErrInvalidSyntax   = errors.New("ERR invalid RESP syntax")
	ErrIncompleteData  = errors.New("ERR incomplete RESP command")
	ErrExpectedArray   = errors.New("ERR expected array of bulk strings")
	ErrExpectedBulkStr = errors.New("ERR expected bulk string")
)

// Type represents the type of a RESP value
type Type byte

const (
	TypeSimpleString Type = SimpleStringPrefix
	TypeError        Type = ErrorPrefix
	TypeInteger      Type = IntegerPrefix
	TypeBulkString   Type = BulkStringPrefix
	TypeArray        Type = ArrayPrefix
	TypeNull         Type = 0
)

// Value represents an arbitrary RESP v2 value
type Value struct {
	Typ   Type
	Str   string
	Num   int64
	Bulk  []byte
	Array []Value
	Null  bool
}

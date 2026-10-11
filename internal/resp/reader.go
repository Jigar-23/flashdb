package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Reader reads and decodes RESP values from an input stream
type Reader struct {
	reader *bufio.Reader
}

// NewReader creates a new RESP Reader
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		reader: bufio.NewReader(rd),
	}
}

// ReadLine reads a line terminated by \r\n
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n < 2 || line[n-2] != '\r' {
		return nil, ErrInvalidSyntax
	}
	return line[:n-2], nil
}

// ReadValue reads the next RESP Value from the stream
func (r *Reader) ReadValue() (Value, error) {
	prefix, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case SimpleStringPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Typ: TypeSimpleString, Str: string(line)}, nil

	case ErrorPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Typ: TypeError, Str: string(line)}, nil

	case IntegerPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		num, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, ErrInvalidSyntax
		}
		return Value{Typ: TypeInteger, Num: num}, nil

	case BulkStringPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return Value{}, ErrInvalidSyntax
		}
		if length == -1 {
			return Value{Typ: TypeBulkString, Null: true}, nil
		}
		buf := make([]byte, length+2) // bulk data + \r\n
		if _, err := io.ReadFull(r.reader, buf); err != nil {
			return Value{}, err
		}
		if buf[length] != '\r' || buf[length+1] != '\n' {
			return Value{}, ErrInvalidSyntax
		}
		return Value{Typ: TypeBulkString, Bulk: buf[:length]}, nil

	case ArrayPrefix:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		count, err := strconv.Atoi(string(line))
		if err != nil {
			return Value{}, ErrInvalidSyntax
		}
		if count == -1 {
			return Value{Typ: TypeArray, Null: true}, nil
		}
		arr := make([]Value, count)
		for i := 0; i < count; i++ {
			val, err := r.ReadValue()
			if err != nil {
				return Value{}, err
			}
			arr[i] = val
		}
		return Value{Typ: TypeArray, Array: arr}, nil

	default:
		return Value{}, fmt.Errorf("%w: unknown prefix %q", ErrInvalidSyntax, prefix)
	}
}

// ReadCommand parses a command array (*N\r\n$M\r\n...) into string arguments
func (r *Reader) ReadCommand() ([]string, error) {
	val, err := r.ReadValue()
	if err != nil {
		return nil, err
	}
	if val.Typ != TypeArray {
		return nil, ErrExpectedArray
	}
	args := make([]string, len(val.Array))
	for i, item := range val.Array {
		if item.Typ != TypeBulkString && item.Typ != TypeSimpleString {
			return nil, ErrExpectedBulkStr
		}
		if item.Typ == TypeBulkString {
			args[i] = string(item.Bulk)
		} else {
			args[i] = item.Str
		}
	}
	return args, nil
}

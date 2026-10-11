package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Writer writes and serializes RESP values to an output stream
type Writer struct {
	writer *bufio.Writer
}

// NewWriter creates a new RESP Writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: bufio.NewWriter(w),
	}
}

// Flush flushes buffered bytes to the underlying stream
func (w *Writer) Flush() error {
	return w.writer.Flush()
}

// WriteValue serializes any RESP Value into the stream
func (w *Writer) WriteValue(v Value) error {
	if v.Null {
		switch v.Typ {
		case TypeArray:
			return w.WriteNullArray()
		default:
			return w.WriteNull()
		}
	}

	switch v.Typ {
	case TypeSimpleString:
		return w.WriteSimpleString(v.Str)
	case TypeError:
		return w.WriteError(v.Str)
	case TypeInteger:
		return w.WriteInteger(v.Num)
	case TypeBulkString:
		return w.WriteBulk(v.Bulk)
	case TypeArray:
		return w.WriteArray(v.Array)
	default:
		return fmt.Errorf("unknown RESP type: %v", v.Typ)
	}
}

// WriteSimpleString writes +<str>\r\n
func (w *Writer) WriteSimpleString(s string) error {
	_, err := w.writer.WriteString("+" + s + "\r\n")
	return err
}

// WriteError writes -<msg>\r\n
func (w *Writer) WriteError(msg string) error {
	_, err := w.writer.WriteString("-" + msg + "\r\n")
	return err
}

// WriteInteger writes :<num>\r\n
func (w *Writer) WriteInteger(num int64) error {
	_, err := w.writer.WriteString(":" + strconv.FormatInt(num, 10) + "\r\n")
	return err
}

// WriteBulk writes $<len>\r\n<data>\r\n
func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		return w.WriteNull()
	}
	header := "$" + strconv.Itoa(len(b)) + "\r\n"
	if _, err := w.writer.WriteString(header); err != nil {
		return err
	}
	if _, err := w.writer.Write(b); err != nil {
		return err
	}
	_, err := w.writer.WriteString("\r\n")
	return err
}

// WriteBulkString is a convenience for string data
func (w *Writer) WriteBulkString(s string) error {
	return w.WriteBulk([]byte(s))
}

// WriteNull writes $-1\r\n
func (w *Writer) WriteNull() error {
	_, err := w.writer.WriteString("$-1\r\n")
	return err
}

// WriteNullArray writes *-1\r\n
func (w *Writer) WriteNullArray() error {
	_, err := w.writer.WriteString("*-1\r\n")
	return err
}

// WriteArray writes *<count>\r\n followed by each element
func (w *Writer) WriteArray(arr []Value) error {
	header := "*" + strconv.Itoa(len(arr)) + "\r\n"
	if _, err := w.writer.WriteString(header); err != nil {
		return err
	}
	for _, val := range arr {
		if err := w.WriteValue(val); err != nil {
			return err
		}
	}
	return nil
}

// Convenience helpers to construct Values
func MakeSimpleString(s string) Value {
	return Value{Typ: TypeSimpleString, Str: s}
}

func MakeError(err string) Value {
	return Value{Typ: TypeError, Str: err}
}

func MakeInteger(num int64) Value {
	return Value{Typ: TypeInteger, Num: num}
}

func MakeBulk(b []byte) Value {
	return Value{Typ: TypeBulkString, Bulk: b}
}

func MakeBulkString(s string) Value {
	return Value{Typ: TypeBulkString, Bulk: []byte(s)}
}

func MakeNull() Value {
	return Value{Typ: TypeBulkString, Null: true}
}

func MakeNullArray() Value {
	return Value{Typ: TypeArray, Null: true}
}

func MakeArray(values []Value) Value {
	return Value{Typ: TypeArray, Array: values}
}

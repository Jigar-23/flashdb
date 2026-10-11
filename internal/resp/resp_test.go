package resp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRESPReaderWriter(t *testing.T) {
	tests := []struct {
		name  string
		input Value
		wire  string
	}{
		{
			name:  "Simple String",
			input: MakeSimpleString("OK"),
			wire:  "+OK\r\n",
		},
		{
			name:  "Error",
			input: MakeError("ERR unknown command"),
			wire:  "-ERR unknown command\r\n",
		},
		{
			name:  "Integer positive",
			input: MakeInteger(1000),
			wire:  ":1000\r\n",
		},
		{
			name:  "Integer negative",
			input: MakeInteger(-42),
			wire:  ":-42\r\n",
		},
		{
			name:  "Bulk String",
			input: MakeBulkString("foobar"),
			wire:  "$6\r\nfoobar\r\n",
		},
		{
			name:  "Bulk String empty",
			input: MakeBulkString(""),
			wire:  "$0\r\n\r\n",
		},
		{
			name:  "Null Bulk String",
			input: MakeNull(),
			wire:  "$-1\r\n",
		},
		{
			name:  "Null Array",
			input: MakeNullArray(),
			wire:  "*-1\r\n",
		},
		{
			name: "Array of mixed elements",
			input: MakeArray([]Value{
				MakeBulkString("echo"),
				MakeBulkString("hello world"),
			}),
			wire: "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Writer
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.WriteValue(tt.input); err != nil {
				t.Fatalf("WriteValue failed: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}
			if buf.String() != tt.wire {
				t.Errorf("wire mismatch: got %q, want %q", buf.String(), tt.wire)
			}

			// Test Reader
			r := NewReader(bytes.NewReader(buf.Bytes()))
			got, err := r.ReadValue()
			if err != nil {
				t.Fatalf("ReadValue failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.input) {
				t.Errorf("value mismatch: got %+v, want %+v", got, tt.input)
			}
		})
	}
}

func TestReadCommand(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := NewReader(bytes.NewReader([]byte(raw)))
	cmd, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand failed: %v", err)
	}
	expected := []string{"SET", "foo", "bar"}
	if !reflect.DeepEqual(cmd, expected) {
		t.Errorf("got %v, want %v", cmd, expected)
	}
}

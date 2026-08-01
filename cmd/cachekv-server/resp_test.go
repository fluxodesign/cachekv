package main

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRespReaderMultiBulk(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			"set",
			"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			[]string{"SET", "foo", "bar"},
		},
		{
			"single argument",
			"*1\r\n$4\r\nPING\r\n",
			[]string{"PING"},
		},
		{
			"empty bulk string",
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n",
			[]string{"SET", "k", ""},
		},
		{
			"binary safe payload",
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\na\r\nb\x00c\r\n",
			[]string{"SET", "k", "a\r\nb\x00c"},
		},
		{
			"empty and null arrays are skipped",
			"*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n",
			[]string{"PING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newRespReader(strings.NewReader(tt.input)).ReadCommand()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRespReaderPipelining(t *testing.T) {
	reader := newRespReader(strings.NewReader(
		"*1\r\n$4\r\nPING\r\n" +
			"GET foo\r\n" +
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
	))

	for _, want := range [][]string{{"PING"}, {"GET", "foo"}, {"SET", "k", "v"}} {
		got, err := reader.ReadCommand()
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	}

	_, err := reader.ReadCommand()
	assert.ErrorIs(t, err, io.EOF)
}

func TestRespReaderInline(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"plain", "SET foo bar\r\n", []string{"SET", "foo", "bar"}},
		{"extra whitespace", "  SET \t foo   bar  \r\n", []string{"SET", "foo", "bar"}},
		{"bare LF terminator", "SET foo bar\n", []string{"SET", "foo", "bar"}},
		{"blank lines skipped", "\r\n   \r\n\nPING\r\n", []string{"PING"}},
		{"double quotes", `SET k "hello world"` + "\r\n", []string{"SET", "k", "hello world"}},
		{"escapes in double quotes", `SET k "a\tb\nc"` + "\r\n", []string{"SET", "k", "a\tb\nc"}},
		{"hex escape", `SET k "\x41\x2f"` + "\r\n", []string{"SET", "k", "A/"}},
		{"single quotes keep backslashes", `SET k 'a\tb'` + "\r\n", []string{"SET", "k", `a\tb`}},
		{"escaped single quote", `SET k 'it\'s'` + "\r\n", []string{"SET", "k", "it's"}},
		{"empty quoted argument", `SET k ""` + "\r\n", []string{"SET", "k", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newRespReader(strings.NewReader(tt.input)).ReadCommand()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRespReaderProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"non numeric multibulk length", "*x\r\n", "invalid multibulk length"},
		{"oversized multibulk length", "*1048577\r\n", "invalid multibulk length"},
		{"missing bulk header", "*1\r\nPING\r\n", "expected '$', got 'P'"},
		{"non numeric bulk length", "*1\r\n$x\r\n", "invalid bulk length"},
		{"negative bulk length", "*1\r\n$-1\r\n", "invalid bulk length"},
		{"oversized bulk length", "*1\r\n$67108865\r\n", "invalid bulk length"},
		{"bulk not CRLF terminated", "*1\r\n$4\r\nPINGX\r\n", "expected CRLF after bulk string"},
		{"bare LF in multibulk header", "*1\n$4\r\nPING\r\n", "expected CRLF terminated line"},
		{"bare LF in bulk header", "*1\r\n$4\nPING\r\n", "expected CRLF terminated line"},
		{"unbalanced quote", "SET k \"unterminated\r\n", "unbalanced quotes in request"},
		{"text after closing quote", "SET k \"a\"b\r\n", "unbalanced quotes in request"},
		{"oversized inline command", strings.Repeat("a", maxLineLength+1) + "\r\n", "too big inline request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newRespReader(strings.NewReader(tt.input)).ReadCommand()
			if !assert.Error(t, err) {
				return
			}
			var protoErr protocolError
			assert.ErrorAs(t, err, &protoErr)
			assert.Equal(t, tt.want, protoErr.Error())
		})
	}
}

func TestRespReaderTruncatedCommand(t *testing.T) {
	// A command that stops mid-flight is a broken stream, not a clean close,
	// so it must not look like EOF to the connection loop.
	for _, input := range []string{"*2\r\n$3\r\nGET\r\n", "*1\r\n$4\r\nPI"} {
		_, err := newRespReader(strings.NewReader(input)).ReadCommand()
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	}
}

func TestRespReaderLongBulkStringSpansBuffer(t *testing.T) {
	// Larger than bufio's default 4 KiB buffer, so io.ReadFull has to make
	// several trips.
	value := strings.Repeat("x", 100_000)
	input := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\n" + value + "\r\n"

	got, err := newRespReader(strings.NewReader(input)).ReadCommand()
	assert.NoError(t, err)
	assert.Equal(t, []string{"SET", "k", value}, got)
}

func TestRespReaderLongInlineCommandSpansBuffer(t *testing.T) {
	// An inline line longer than bufio's buffer but under maxLineLength must
	// be reassembled across ReadSlice calls.
	value := strings.Repeat("y", 10_000)

	got, err := newRespReader(strings.NewReader("SET k " + value + "\r\n")).ReadCommand()
	assert.NoError(t, err)
	assert.Equal(t, []string{"SET", "k", value}, got)
}

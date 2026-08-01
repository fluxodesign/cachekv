package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Protocol limits, mirroring Redis's guards against a client (or a stray port
// scanner) announcing an enormous payload and making us allocate it.
const (
	// maxMultiBulkLength caps how many arguments one command may carry.
	maxMultiBulkLength = 1024 * 1024
	// maxBulkLength caps a single argument. Redis defaults to 512 MiB
	// (proto-max-bulk-len); we are stricter because every value here lives in
	// memory and is JSON-serialised whole on save.
	maxBulkLength = 64 * 1024 * 1024
	// maxLineLength caps inline commands and the `*`/`$` header lines, which
	// are read into a growable buffer before their length is known.
	maxLineLength = 64 * 1024
)

// protocolError is a malformed request: recoverable in the sense that we can
// tell the client what went wrong, but the stream is out of sync afterwards so
// the caller closes the connection. Anything else returned by respReader is a
// transport error (EOF, reset, ...).
type protocolError string

func (e protocolError) Error() string { return string(e) }

var errUnbalancedQuotes = protocolError("unbalanced quotes in request")

// respReader parses the Redis serialisation protocol off a connection. It
// accepts both forms a real Redis server accepts: the multi-bulk arrays every
// client library sends, and the inline (telnet-style) commands people type by
// hand.
type respReader struct {
	r *bufio.Reader
}

func newRespReader(r io.Reader) *respReader {
	return &respReader{r: bufio.NewReader(r)}
}

// Buffered reports how many bytes are already in the read buffer, which the
// caller uses to decide whether a pipelined batch is still in flight.
func (rr *respReader) Buffered() int { return rr.r.Buffered() }

// ReadCommand returns the next command as command name plus arguments. Empty
// requests (blank inline lines, `*0`, `*-1`) are skipped rather than returned,
// so the result always has at least one element.
func (rr *respReader) ReadCommand() ([]string, error) {
	for {
		prefix, err := rr.r.Peek(1)
		if err != nil {
			return nil, err
		}

		var args []string
		if prefix[0] == '*' {
			args, err = rr.readMultiBulk()
		} else {
			args, err = rr.readInline()
		}
		if err != nil {
			return nil, err
		}
		if len(args) > 0 {
			return args, nil
		}
	}
}

// readInline reads one whitespace-separated command line.
func (rr *respReader) readInline() ([]string, error) {
	line, err := rr.readLine("too big inline request", false)
	if err != nil {
		return nil, err
	}
	return splitInlineArgs(string(line))
}

// readMultiBulk reads a `*N\r\n` header followed by N bulk strings.
func (rr *respReader) readMultiBulk() ([]string, error) {
	line, err := rr.readLine("too big mbulk count string", true)
	if err != nil {
		return nil, err
	}

	// The leading '*' is guaranteed by ReadCommand's peek.
	count, err := strconv.Atoi(string(line[1:]))
	if err != nil || count > maxMultiBulkLength {
		return nil, protocolError("invalid multibulk length")
	}
	if count <= 0 {
		// `*0` is an empty command and `*-1` a null array; both are no-ops.
		return nil, nil
	}

	// Grow into the announced count rather than trusting it up front: a
	// twelve-byte header should not buy the sender a multi-megabyte slice.
	args := make([]string, 0, min(count, 64))
	for range count {
		arg, err := rr.readBulkString()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

// readBulkString reads one `$N\r\n<N bytes>\r\n` argument. Bulk strings are
// binary-safe: the payload may contain CRLF, NUL, or anything else.
func (rr *respReader) readBulkString() (string, error) {
	line, err := rr.readLine("too big bulk count string", true)
	if err != nil {
		// A half-delivered command is a broken stream, not a clean close.
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		got := "end of line"
		if len(line) > 0 {
			got = string(line[0])
		}
		return "", protocolError(fmt.Sprintf("expected '$', got '%s'", got))
	}

	length, err := strconv.Atoi(string(line[1:]))
	if err != nil || length < 0 || length > maxBulkLength {
		return "", protocolError("invalid bulk length")
	}

	buf := make([]byte, length+2) // payload plus its trailing CRLF
	if _, err := io.ReadFull(rr.r, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return "", err
	}
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return "", protocolError("expected CRLF after bulk string")
	}
	return string(buf[:length]), nil
}

// readLine reads one LF-terminated protocol line and returns it without the
// terminator, refusing to buffer more than maxLineLength bytes. RESP headers
// pass requireCR because a lone LF there means the sender is out of sync;
// inline commands don't, since a hand-typed line may well arrive without the
// carriage return. The returned slice points into the reader's buffer and is
// only valid until the next read, so callers must copy anything they keep.
func (rr *respReader) readLine(tooLong string, requireCR bool) ([]byte, error) {
	var line []byte
	for {
		chunk, err := rr.r.ReadSlice('\n')
		if len(line)+len(chunk) > maxLineLength {
			return nil, protocolError(tooLong)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			// No terminator in this bufferful; keep the bytes and read on.
			line = append(line, chunk...)
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(line) > 0 {
			chunk = append(line, chunk...)
		}

		chunk = chunk[:len(chunk)-1] // drop the LF
		if len(chunk) > 0 && chunk[len(chunk)-1] == '\r' {
			return chunk[:len(chunk)-1], nil
		}
		if requireCR {
			return nil, protocolError("expected CRLF terminated line")
		}
		return chunk, nil
	}
}

// splitInlineArgs splits a telnet-style command the way Redis's sdssplitargs
// does: whitespace separates arguments, double quotes take the usual C escapes
// (\n, \t, \xHH, ...) and single quotes take only \'. A quote must be closed,
// and closing it must end the argument.
func splitInlineArgs(line string) ([]string, error) {
	var args []string
	for i := 0; ; {
		for i < len(line) && isInlineSpace(line[i]) {
			i++
		}
		if i >= len(line) {
			return args, nil
		}

		var (
			arg      []byte
			inQuote  bool
			inSingle bool
		)
		for done := false; !done; i++ {
			if i >= len(line) {
				if inQuote || inSingle {
					return nil, errUnbalancedQuotes
				}
				break
			}

			c := line[i]
			switch {
			case inQuote:
				switch {
				case c == '\\' && i+3 < len(line) && line[i+1] == 'x' &&
					isHexDigit(line[i+2]) && isHexDigit(line[i+3]):
					arg = append(arg, hexDigit(line[i+2])<<4|hexDigit(line[i+3]))
					i += 3
				case c == '\\' && i+1 < len(line):
					i++
					arg = append(arg, unescape(line[i]))
				case c == '"':
					if i+1 < len(line) && !isInlineSpace(line[i+1]) {
						return nil, errUnbalancedQuotes
					}
					done = true
				default:
					arg = append(arg, c)
				}
			case inSingle:
				switch {
				case c == '\\' && i+1 < len(line) && line[i+1] == '\'':
					i++
					arg = append(arg, '\'')
				case c == '\'':
					if i+1 < len(line) && !isInlineSpace(line[i+1]) {
						return nil, errUnbalancedQuotes
					}
					done = true
				default:
					arg = append(arg, c)
				}
			default:
				switch c {
				case '"':
					inQuote = true
				case '\'':
					inSingle = true
				case 0:
					done = true
				default:
					if isInlineSpace(c) {
						done = true
					} else {
						arg = append(arg, c)
					}
				}
			}
		}
		args = append(args, string(arg))
	}
}

func isInlineSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}
	return false
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func hexDigit(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	default:
		return c - '0'
	}
}

func unescape(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'b':
		return '\b'
	case 'a':
		return '\a'
	default:
		return c
	}
}

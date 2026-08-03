package main

import (
	"fmt"
	"strings"
)

const (
	okReply           = "+OK\r\n"
	pongReply         = "+PONG\r\n"
	nilBulk           = "$-1\r\n" // Redis standard for (nil)
	noneReply         = "+none\r\n"
	errWrongType      = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	errNoAuth         = "-NOAUTH Authentication required.\r\n"
	emptyArray        = "*0\r\n"
	nilArray          = "*-1\r\n"
	errSyntax         = "-ERR syntax error\r\n"
	errNotInteger     = "-ERR value is not an integer or out of range\r\n"
	errMustBePositive = "-ERR value is out of range, must be positive\r\n"
	errNotFloat       = "-ERR value is not a valid float\r\n"
	errMinMaxNotFloat = "-ERR min or max is not a float\r\n"
	errOverflow       = "-ERR increment or decrement would overflow\r\n"
	errNoSuchKey      = "-ERR no such key\r\n"
)

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func integer(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// integer64 is integer's int64 counterpart, for replies (INCR/INCRBY and
// friends) that can genuinely exceed a 32-bit int.
func integer64(n int64) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func wrongArgs(cmd string) string {
	return fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", cmd)
}

func invalidExpireErr(cmd string) string {
	return fmt.Sprintf("-ERR invalid expire time in '%s' command\r\n", cmd)
}

// arrayReply composes elems — each already a complete RESP token
// (bulkString(x), integer(n), or a nested arrayReply(...)) — into one RESP
// array, the same way RESP itself nests.
func arrayReply(elems ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(elems))
	for _, e := range elems {
		b.WriteString(e)
	}
	return b.String()
}

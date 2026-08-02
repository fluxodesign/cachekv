package main

import "fmt"

const (
	okReply      = "+OK\r\n"
	pongReply    = "+PONG\r\n"
	nilBulk      = "$-1\r\n" // Redis standard for (nil)
	noneReply    = "+none\r\n"
	errWrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
)

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func integer(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func wrongArgs(cmd string) string {
	return fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", cmd)
}

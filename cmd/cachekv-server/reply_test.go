package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrayReply(t *testing.T) {
	assert.Equal(t, "*0\r\n", arrayReply())
	assert.Equal(t, emptyArray, arrayReply())

	assert.Equal(t, "*2\r\n$1\r\na\r\n:1\r\n", arrayReply(bulkString("a"), integer(1)))

	// Nesting composes the same way RESP itself nests.
	nested := arrayReply(bulkString("x"), arrayReply(integer(1), integer(2)))
	assert.Equal(t, "*2\r\n$1\r\nx\r\n*2\r\n:1\r\n:2\r\n", nested)
}

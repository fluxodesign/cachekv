package main

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startHandleConnection wires handleConnection to one end of a net.Pipe and a
// fake stateProcessor that replies okReply to anything it receives on
// cmdChan. Tests only need to assert what handleConnection did *before*
// forwarding a command — the real command handlers are covered separately in
// datastore_test.go.
func startHandleConnection(t *testing.T, requirePass string) (send func(cmdLine string) string, forwarded func() int32) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	require.NoError(t, clientConn.SetDeadline(deadline))
	require.NoError(t, serverConn.SetDeadline(deadline))

	cmdChan := make(chan CkvCommand)
	var count atomic.Int32
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for cmd := range cmdChan {
			count.Add(1)
			cmd.Resp <- okReply
		}
	}()

	connDone := make(chan struct{})
	go func() {
		defer close(connDone)
		handleConnection(serverConn, cmdChan, requirePass)
		close(cmdChan)
	}()

	reader := bufio.NewReader(clientConn)
	t.Cleanup(func() {
		_ = clientConn.Close()
		<-connDone
		<-consumerDone
	})

	send = func(cmdLine string) string {
		t.Helper()
		_, err := clientConn.Write([]byte(cmdLine + "\r\n"))
		require.NoError(t, err)
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		return line
	}
	return send, count.Load
}

func TestHandleConnectionNoRequirePassForwardsImmediately(t *testing.T) {
	send, forwarded := startHandleConnection(t, "")

	assert.Equal(t, okReply, send("GET k"))
	assert.EqualValues(t, 1, forwarded())
}

func TestHandleConnectionRequirePassGatesUntilAuthenticated(t *testing.T) {
	send, forwarded := startHandleConnection(t, "secret")

	assert.Equal(t, errNoAuth, send("GET k"))
	assert.EqualValues(t, 0, forwarded())

	assert.Equal(t, "-ERR invalid password\r\n", send("AUTH wrong"))
	assert.Equal(t, errNoAuth, send("GET k"))
	assert.EqualValues(t, 0, forwarded())

	assert.Equal(t, okReply, send("AUTH secret"))
	assert.Equal(t, okReply, send("GET k"))
	assert.EqualValues(t, 1, forwarded())
}

func TestHandleConnectionAuthReAuthenticatesOnEachCall(t *testing.T) {
	send, forwarded := startHandleConnection(t, "secret")

	assert.Equal(t, okReply, send("AUTH secret"))
	assert.Equal(t, okReply, send("GET k"))
	assert.EqualValues(t, 1, forwarded())

	// A wrong password re-gates a previously-authenticated connection: AUTH
	// always re-validates, it doesn't just latch true once.
	assert.Equal(t, "-ERR invalid password\r\n", send("AUTH wrong"))
	assert.Equal(t, errNoAuth, send("GET k"))
	assert.EqualValues(t, 1, forwarded())
}

func TestHandleConnectionAuthWithNoPasswordConfigured(t *testing.T) {
	send, _ := startHandleConnection(t, "")

	assert.Equal(t, "-ERR Client sent AUTH, but no password is set.\r\n", send("AUTH anything"))
}

func TestHandleConnectionAuthWrongArity(t *testing.T) {
	send, _ := startHandleConnection(t, "secret")

	assert.Equal(t, wrongArgs("auth"), send("AUTH"))
	assert.Equal(t, wrongArgs("auth"), send("AUTH a b"))
}

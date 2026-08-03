package main

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readReply reads one complete RESP reply of any type off reader — scalar
// (+/-/:) is one line, bulk ($) is a length line plus payload, array (*) is a
// count line plus that many nested replies. Returned verbatim (including all
// framing), so it can be compared byte-for-byte against what reply.go's own
// constructors (bulkString, integer, arrayReply) would produce.
func readReply(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		require.NoError(t, err)
		if n < 0 {
			return line
		}
		buf := make([]byte, n+2) // payload plus trailing CRLF
		_, err = io.ReadFull(reader, buf)
		require.NoError(t, err)
		return line + string(buf)
	case '*':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		require.NoError(t, err)
		reply := line
		for range n {
			reply += readReply(t, reader)
		}
		return reply
	default:
		t.Fatalf("unexpected reply prefix in %q", line)
		return ""
	}
}

// harness wires handleConnection to one end of a net.Pipe and a fake
// stateProcessor that replies okReply to anything it receives on cmdChan.
// Tests only need to assert what handleConnection did *before* forwarding a
// command — the real command handlers are covered separately in
// datastore_test.go.
type harness struct {
	// send writes cmdLine and reads back exactly one full RESP reply.
	send func(cmdLine string) string
	// forwarded reports how many commands actually reached the fake
	// stateProcessor consumer via cmdChan.
	forwarded func() int32
	// registry is the same *clientRegistry handleConnection registered
	// into, for asserting CLIENT LIST content directly.
	registry *clientRegistry
	// shutdownCalled reports whether the injected shutdown trigger fired —
	// no real syscall.Kill ever runs in these tests.
	shutdownCalled func() bool
	// clientConn is exposed so tests can probe connection teardown (QUIT).
	clientConn net.Conn
}

func startHandleConnection(t *testing.T, requirePass string) *harness {
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

	registry := newClientRegistry()
	var shutdownCalled atomic.Bool

	connDone := make(chan struct{})
	go func() {
		defer close(connDone)
		handleConnection(serverConn, cmdChan, requirePass, registry, func() {
			shutdownCalled.Store(true)
		})
		close(cmdChan)
	}()

	reader := bufio.NewReader(clientConn)
	t.Cleanup(func() {
		_ = clientConn.Close()
		<-connDone
		<-consumerDone
	})

	return &harness{
		send: func(cmdLine string) string {
			t.Helper()
			_, err := clientConn.Write([]byte(cmdLine + "\r\n"))
			require.NoError(t, err)
			return readReply(t, reader)
		},
		forwarded:      count.Load,
		registry:       registry,
		shutdownCalled: shutdownCalled.Load,
		clientConn:     clientConn,
	}
}

// parseInteger extracts n from a ":<n>\r\n" integer reply.
func parseInteger(t *testing.T, reply string) int {
	t.Helper()
	trimmed := strings.TrimSuffix(strings.TrimPrefix(reply, ":"), "\r\n")
	n, err := strconv.Atoi(trimmed)
	require.NoError(t, err)
	return n
}

func TestHandleConnectionNoRequirePassForwardsImmediately(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, okReply, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())
}

func TestHandleConnectionRequirePassGatesUntilAuthenticated(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, errNoAuth, h.send("GET k"))
	assert.EqualValues(t, 0, h.forwarded())

	assert.Equal(t, "-ERR invalid password\r\n", h.send("AUTH wrong"))
	assert.Equal(t, errNoAuth, h.send("GET k"))
	assert.EqualValues(t, 0, h.forwarded())

	assert.Equal(t, okReply, h.send("AUTH secret"))
	assert.Equal(t, okReply, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())
}

func TestHandleConnectionAuthReAuthenticatesOnEachCall(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, okReply, h.send("AUTH secret"))
	assert.Equal(t, okReply, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())

	// A wrong password re-gates a previously-authenticated connection: AUTH
	// always re-validates, it doesn't just latch true once.
	assert.Equal(t, "-ERR invalid password\r\n", h.send("AUTH wrong"))
	assert.Equal(t, errNoAuth, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())
}

func TestHandleConnectionAuthWithNoPasswordConfigured(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, "-ERR Client sent AUTH, but no password is set.\r\n", h.send("AUTH anything"))
}

func TestHandleConnectionAuthWrongArity(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, wrongArgs("auth"), h.send("AUTH"))
	assert.Equal(t, wrongArgs("auth"), h.send("AUTH a b"))
}

func TestHandleConnectionQuitClosesConnection(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, okReply, h.send("QUIT"))

	// handleConnection has returned and closed its end; the peer's next
	// write must fail rather than silently succeed into nowhere.
	deadline := time.Now().Add(2 * time.Second)
	require.NoError(t, h.clientConn.SetWriteDeadline(deadline))
	_, err := h.clientConn.Write([]byte("PING\r\n"))
	assert.Error(t, err)
}

func TestHandleConnectionHelloReportsProtoTwoRegardlessOfRequest(t *testing.T) {
	for _, cmdLine := range []string{"HELLO", "HELLO 2", "HELLO 3"} {
		t.Run(cmdLine, func(t *testing.T) {
			h := startHandleConnection(t, "")

			id := parseInteger(t, h.send("CLIENT ID"))

			expected := arrayReply(
				bulkString("server"), bulkString("cachekv"),
				bulkString("version"), bulkString("0.1.0"),
				bulkString("proto"), integer(2),
				bulkString("id"), integer(id),
				bulkString("mode"), bulkString("standalone"),
				bulkString("role"), bulkString("master"),
				bulkString("modules"), emptyArray,
			)
			assert.Equal(t, expected, h.send(cmdLine))
		})
	}
}

func TestHandleConnectionHelloRejectsUnsupportedProtover(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, "-NOPROTO unsupported protocol version\r\n", h.send("HELLO 9"))
}

func TestHandleConnectionHelloRequiresAuthWhenPasswordSet(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, errNoAuth, h.send("HELLO 2"))
}

func TestHandleConnectionHelloAuthClauseAuthenticates(t *testing.T) {
	h := startHandleConnection(t, "secret")

	reply := h.send("HELLO 2 AUTH default secret")
	assert.True(t, strings.HasPrefix(reply, "*"), "HELLO should succeed once the AUTH clause checks out, got %q", reply)

	// The connection is now authenticated: a following data command is
	// forwarded instead of NOAUTH'd.
	assert.Equal(t, okReply, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())
}

func TestHandleConnectionHelloAuthClauseWrongPassword(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t,
		"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
		h.send("HELLO 2 AUTH default wrong"))
	assert.Equal(t, errNoAuth, h.send("GET k"))
}

func TestHandleConnectionClientIDGetnameSetname(t *testing.T) {
	h := startHandleConnection(t, "")

	id1 := parseInteger(t, h.send("CLIENT ID"))
	id2 := parseInteger(t, h.send("CLIENT ID"))
	assert.Equal(t, id1, id2, "CLIENT ID is stable across calls on the same connection")

	assert.Equal(t, bulkString(""), h.send("CLIENT GETNAME"))

	assert.Equal(t, okReply, h.send("CLIENT SETNAME myapp"))
	assert.Equal(t, bulkString("myapp"), h.send("CLIENT GETNAME"))
}

func TestHandleConnectionClientSetnameRejectsSpaces(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t,
		"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
		h.send(`CLIENT SETNAME "has space"`))
}

func TestHandleConnectionClientList(t *testing.T) {
	h := startHandleConnection(t, "")

	id := parseInteger(t, h.send("CLIENT ID"))
	h.send("CLIENT SETNAME myapp")

	assert.Contains(t, h.registry.list(), "id="+strconv.Itoa(id))
	assert.Contains(t, h.registry.list(), "name=myapp")

	reply := h.send("CLIENT LIST")
	assert.Contains(t, reply, "name=myapp")
}

func TestHandleConnectionClientListRejectsFilters(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, "-ERR syntax error\r\n", h.send("CLIENT LIST ID 1"))
}

func TestHandleConnectionShutdownGatedByAuth(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, errNoAuth, h.send("SHUTDOWN"))
	assert.False(t, h.shutdownCalled())
}

func TestHandleConnectionShutdownTriggersInjectedFunc(t *testing.T) {
	h := startHandleConnection(t, "")

	_, err := h.clientConn.Write([]byte("SHUTDOWN\r\n"))
	require.NoError(t, err)

	require.Eventually(t, h.shutdownCalled, 2*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, 0, h.forwarded())
}

func TestHandleConnectionResetIsPreAuthReachable(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, "+RESET\r\n", h.send("RESET"))
}

func TestHandleConnectionResetDeauthenticates(t *testing.T) {
	h := startHandleConnection(t, "secret")

	assert.Equal(t, okReply, h.send("AUTH secret"))
	assert.Equal(t, okReply, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded())

	assert.Equal(t, "+RESET\r\n", h.send("RESET"))

	assert.Equal(t, errNoAuth, h.send("GET k"))
	assert.EqualValues(t, 1, h.forwarded()) // unchanged: the post-RESET GET never reached cmdChan
}

func TestHandleConnectionResetClearsName(t *testing.T) {
	h := startHandleConnection(t, "")

	assert.Equal(t, okReply, h.send("CLIENT SETNAME myapp"))
	assert.Equal(t, bulkString("myapp"), h.send("CLIENT GETNAME"))

	assert.Equal(t, "+RESET\r\n", h.send("RESET"))

	assert.Equal(t, bulkString(""), h.send("CLIENT GETNAME"))
}

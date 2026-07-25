// Command cachekv-cli is a command-line client for cachekv-server.
//
// Global flags come before the subcommand:
//
//	cachekv-cli --addr localhost:50051 --json get mydb mykey
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/status"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

const (
	defaultAddr = "localhost:50051"
	addrEnvVar  = "CACHEKV_ADDR"
)

// Exit codes: anything the user can fix by re-reading the usage text exits with
// exitUsage, everything else (RPC failures, unreadable files) with exitErr.
const (
	exitOK    = 0
	exitErr   = 1
	exitUsage = 2
)

// env carries everything a subcommand needs: a connected client, where to write
// output, and the global output/timeout settings.
type env struct {
	client  cachekvv1.CacheKVClient
	out     io.Writer
	jsonOut bool
	timeout time.Duration
}

// ctx returns the context for a single RPC. For the streaming commands this
// bounds the whole stream, not each message, so `all` on a large database may
// need a longer --timeout.
func (e *env) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), e.timeout)
}

// command is one entry in the subcommand dispatch table.
type command struct {
	name    string
	usage   string
	summary string
	run     func(e *env, args []string) error
}

var commands = []command{
	{"db", "db create <name> [--secure] | db list", "create or list databases", cmdDb},
	{"put", "put <db> <key> <value>", "insert a new entry", cmdPut},
	{"update", "update <db> <key> <value>", "overwrite an existing entry", cmdUpdate},
	{"rm", "rm <db> <key>", "remove an entry", cmdRemove},
	{"batch-insert", "batch-insert <db> --file entries.json", "insert many entries in one call", cmdBatchInsert},
	{"get", "get <db> <key>", "read a single entry", cmdGet},
	{"all", "all <db>", "stream every entry in a database", cmdAll},
	{"config", "config get | config set <json-file>", "read or patch the server configuration", cmdConfig},
	{"metrics", "metrics", "print the server's metrics snapshot", cmdMetrics},
	{"ping", "ping", "check that the server answers", cmdPing},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("cachekv-cli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", envOr(addrEnvVar, defaultAddr), "address of cachekv-server")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON instead of plain text")
	timeout := fs.Duration("timeout", 30*time.Second, "timeout for a single RPC")
	fs.Usage = func() { printUsage(stderr, fs) }

	if err := fs.Parse(args); err != nil {
		// flag has already written the error and the usage text to stderr.
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}

	rest := fs.Args()
	if len(rest) == 0 {
		printUsage(stderr, fs)
		return exitUsage
	}

	cmd := lookup(rest[0])
	if cmd == nil {
		fmt.Fprintf(stderr, "unknown command %q\n\n", rest[0])
		printUsage(stderr, fs)
		return exitUsage
	}

	client, closer, err := dial(*addr)
	if err != nil {
		fmt.Fprintf(stderr, "cannot reach %s: %v\n", *addr, err)
		return exitErr
	}
	defer func() { _ = closer.Close() }()

	e := &env{client: client, out: stdout, jsonOut: *jsonOut, timeout: *timeout}
	if err := cmd.run(e, rest[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(stderr, "usage: cachekv-cli [global flags] %s\n", cmd.usage)
			return exitOK
		}
		var ue usageError
		if errors.As(err, &ue) {
			if msg := ue.Error(); msg != "" {
				fmt.Fprintln(stderr, msg)
			}
			fmt.Fprintf(stderr, "usage: cachekv-cli [global flags] %s\n", cmd.usage)
			return exitUsage
		}
		fmt.Fprintln(stderr, formatRPCError(err))
		return exitErr
	}
	return exitOK
}

func lookup(name string) *command {
	for i := range commands {
		if commands[i].name == name {
			return &commands[i]
		}
	}
	return nil
}

func printUsage(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprintln(w, "usage: cachekv-cli [global flags] <command> [arguments]")
	fmt.Fprintln(w, "\ncommands:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, c := range commands {
		fmt.Fprintf(tw, "  %s\t%s\n", c.usage, c.summary)
	}
	_ = tw.Flush()
	fmt.Fprintln(w, "\nglobal flags:")
	fs.PrintDefaults()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// usageError marks a failure the user can fix by re-reading the usage text.
type usageError struct{ err error }

func (u usageError) Error() string { return u.err.Error() }
func (u usageError) Unwrap() error { return u.err }

func usagef(format string, args ...any) error {
	return usageError{fmt.Errorf(format, args...)}
}

// flagError turns a subcommand FlagSet failure into a usage error, leaving
// -h/--help alone so the caller can print usage and exit successfully.
func flagError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return err
	}
	return usageError{err}
}

// parseFlagsAnywhere lets flags appear before, after, or between positional
// arguments: stdlib flag stops at the first non-flag argument, but
// "db create mydb --secure" reads more naturally than the reverse.
func parseFlagsAnywhere(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// formatRPCError renders a gRPC status as "Code: message", dropping the
// "rpc error: code = ..." prefix the status package produces by default.
func formatRPCError(err error) error {
	if st, ok := status.FromError(err); ok {
		return fmt.Errorf("%s: %s", st.Code(), st.Message())
	}
	return err
}

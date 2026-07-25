package main

import (
	"flag"
	"fmt"
	"io"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

func cmdDb(e *env, args []string) error {
	if len(args) == 0 {
		return usagef("db needs a subcommand")
	}
	switch args[0] {
	case "create":
		return dbCreate(e, args[1:])
	case "list":
		return dbList(e, args[1:])
	default:
		return usagef("unknown db subcommand %q", args[0])
	}
}

func dbCreate(e *env, args []string) error {
	fs := flag.NewFlagSet("db create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	secure := fs.Bool("secure", false, "encrypt the database at rest")
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return flagError(err)
	}
	if len(positional) != 1 {
		return usagef("db create needs exactly one database name, got %d", len(positional))
	}
	name := positional[0]

	ctx, cancel := e.ctx()
	defer cancel()
	req := &cachekvv1.CreateDatabaseRequest{DbName: name, Secure: *secure}
	if _, err := e.client.CreateDatabase(ctx, req); err != nil {
		return err
	}

	return e.emit(okResult{OK: true}, func(w io.Writer) {
		if *secure {
			fmt.Fprintf(w, "created secure database %s\n", name)
			return
		}
		fmt.Fprintf(w, "created database %s\n", name)
	})
}

func dbList(e *env, args []string) error {
	if len(args) != 0 {
		return usagef("db list takes no arguments")
	}

	ctx, cancel := e.ctx()
	defer cancel()
	resp, err := e.client.ListDatabases(ctx, &cachekvv1.ListDatabasesRequest{})
	if err != nil {
		return err
	}

	names := resp.GetNames()
	if names == nil {
		names = []string{} // marshal an empty database list as [], not null
	}
	return e.emit(databasesResult{Databases: names}, func(w io.Writer) {
		if len(names) == 0 {
			fmt.Fprintln(w, "no databases")
			return
		}
		for _, name := range names {
			fmt.Fprintln(w, name)
		}
	})
}

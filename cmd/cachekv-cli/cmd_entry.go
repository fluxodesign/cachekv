package main

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

func cmdPut(e *env, args []string) error {
	if len(args) != 3 {
		return usagef("put needs <db> <key> <value>, got %d arguments", len(args))
	}
	db, key, value := args[0], args[1], []byte(args[2])

	ctx, cancel := e.ctx()
	defer cancel()
	req := &cachekvv1.InsertEntryRequest{DbName: db, Key: key, Value: value}
	if _, err := e.client.InsertEntry(ctx, req); err != nil {
		return err
	}

	return e.emit(okResult{OK: true}, func(w io.Writer) {
		fmt.Fprintf(w, "inserted %s into %s\n", key, db)
	})
}

func cmdUpdate(e *env, args []string) error {
	if len(args) != 3 {
		return usagef("update needs <db> <key> <value>, got %d arguments", len(args))
	}
	db, key, value := args[0], args[1], []byte(args[2])

	ctx, cancel := e.ctx()
	defer cancel()
	req := &cachekvv1.UpdateEntryRequest{DbName: db, Key: key, Value: value}
	if _, err := e.client.UpdateEntry(ctx, req); err != nil {
		return err
	}

	return e.emit(okResult{OK: true}, func(w io.Writer) {
		fmt.Fprintf(w, "updated %s in %s\n", key, db)
	})
}

func cmdRemove(e *env, args []string) error {
	if len(args) != 2 {
		return usagef("rm needs <db> <key>, got %d arguments", len(args))
	}
	db, key := args[0], args[1]

	ctx, cancel := e.ctx()
	defer cancel()
	if _, err := e.client.RemoveEntry(ctx, &cachekvv1.RemoveEntryRequest{DbName: db, Key: key}); err != nil {
		return err
	}

	return e.emit(okResult{OK: true}, func(w io.Writer) {
		fmt.Fprintf(w, "removed %s from %s\n", key, db)
	})
}

func cmdGet(e *env, args []string) error {
	if len(args) != 2 {
		return usagef("get needs <db> <key>, got %d arguments", len(args))
	}
	db, key := args[0], args[1]

	ctx, cancel := e.ctx()
	defer cancel()
	resp, err := e.client.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: db, Key: key})
	if err != nil {
		return err
	}

	value := resp.GetValue()
	return e.emit(newEntryResult(key, value), func(w io.Writer) {
		// Write the value verbatim so it can be piped somewhere useful.
		_, _ = w.Write(value)
		fmt.Fprintln(w)
	})
}

func cmdAll(e *env, args []string) error {
	if len(args) != 1 {
		return usagef("all needs <db>, got %d arguments", len(args))
	}
	db := args[0]

	ctx, cancel := e.ctx()
	defer cancel()
	stream, err := e.client.GetAll(ctx, &cachekvv1.GetAllRequest{DbName: db})
	if err != nil {
		return err
	}

	count := 0
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		count++

		if e.jsonOut {
			if err := e.writeJSONLine(newEntryResult(entry.GetKey(), entry.GetValue())); err != nil {
				return err
			}
			continue
		}
		fmt.Fprintf(e.out, "%s\t%s\n", entry.GetKey(), textValue(entry.GetValue()))
	}

	if count == 0 && !e.jsonOut {
		fmt.Fprintln(e.out, "no entries")
	}
	return nil
}

// entryInput is one element of the batch-insert --file array:
//
//	[
//	  {"key": "alpha", "value": "one"},
//	  {"key": "beta",  "value": "AQIDBA==", "encoding": "base64"}
//	]
//
// encoding defaults to "utf8".
type entryInput struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
}

func cmdBatchInsert(e *env, args []string) error {
	fs := flag.NewFlagSet("batch-insert", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	file := fs.String("file", "", `JSON file of entries to insert, or "-" for stdin`)
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return flagError(err)
	}
	if len(positional) != 1 {
		return usagef("batch-insert needs exactly one database name, got %d", len(positional))
	}
	if *file == "" {
		return usagef("batch-insert needs --file")
	}
	db := positional[0]

	entries, err := readEntries(*file)
	if err != nil {
		return err
	}

	ctx, cancel := e.ctx()
	defer cancel()
	resp, err := e.client.BatchInsert(ctx, &cachekvv1.BatchInsertRequest{DbName: db, Entries: entries})
	if err != nil {
		return err
	}

	count := resp.GetCount()
	return e.emit(countResult{OK: true, Count: count}, func(w io.Writer) {
		fmt.Fprintf(w, "inserted %d entries into %s\n", count, db)
	})
}

func readEntries(path string) ([]*cachekvv1.Entry, error) {
	var inputs []entryInput
	if err := decodeJSONFile(path, &inputs); err != nil {
		return nil, err
	}
	name := inputName(path)
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%s contains no entries", name)
	}

	entries := make([]*cachekvv1.Entry, 0, len(inputs))
	seen := make(map[string]struct{}, len(inputs))
	for i, in := range inputs {
		if in.Key == "" {
			return nil, fmt.Errorf("%s: entry %d has an empty key", name, i)
		}
		// The server batches into a map, so a duplicate key would silently
		// drop one of the two values rather than insert both.
		if _, dup := seen[in.Key]; dup {
			return nil, fmt.Errorf("%s: duplicate key %q", name, in.Key)
		}
		seen[in.Key] = struct{}{}

		value, err := decodeEntryValue(in)
		if err != nil {
			return nil, fmt.Errorf("%s: entry %d (%q): %w", name, i, in.Key, err)
		}
		entries = append(entries, &cachekvv1.Entry{Key: in.Key, Value: value})
	}
	return entries, nil
}

func decodeEntryValue(in entryInput) ([]byte, error) {
	switch in.Encoding {
	case "", "utf8":
		return []byte(in.Value), nil
	case "base64":
		value, err := base64.StdEncoding.DecodeString(in.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value: %w", err)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown encoding %q, want \"utf8\" or \"base64\"", in.Encoding)
	}
}

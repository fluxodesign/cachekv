package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"unicode/utf8"
)

// Result types are hand-mapped rather than protojson-marshalled so the JSON
// field names stay stable and script-friendly regardless of the wire format.

// okResult is the shape for commands that either succeed or fail.
type okResult struct {
	OK bool `json:"ok"`
}

type databasesResult struct {
	Databases []string `json:"databases"`
}

type countResult struct {
	OK    bool  `json:"ok"`
	Count int32 `json:"count"`
}

type pingResult struct {
	OK      bool   `json:"ok"`
	Latency string `json:"latency"`
}

// entryResult reports a value as text when it is valid UTF-8 and as base64
// otherwise; encoding says which, so a consumer never has to guess.
type entryResult struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
}

func newEntryResult(key string, value []byte) entryResult {
	if utf8.Valid(value) {
		return entryResult{Key: key, Value: string(value), Encoding: "utf8"}
	}
	return entryResult{Key: key, Value: base64.StdEncoding.EncodeToString(value), Encoding: "base64"}
}

// emit writes v as JSON under --json, otherwise delegates to text for the
// human-readable form.
func (e *env) emit(v any, text func(w io.Writer)) error {
	if e.jsonOut {
		return e.writeJSON(v)
	}
	text(e.out)
	return nil
}

func (e *env) writeJSON(v any) error {
	enc := json.NewEncoder(e.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// writeJSONLine emits one compact object per line. Streaming commands use this
// instead of a single array, which could not be printed until the whole stream
// had been buffered.
func (e *env) writeJSONLine(v any) error {
	return json.NewEncoder(e.out).Encode(v)
}

// textValue renders a value for line-oriented output: raw when it is valid
// UTF-8 free of tabs and newlines (which would break the one-entry-per-line
// format), base64-tagged otherwise.
func textValue(value []byte) string {
	if utf8.Valid(value) && !bytes.ContainsAny(value, "\t\n\r") {
		return string(value)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(value)
}

// inputName is how a path is referred to in error messages.
func inputName(path string) string {
	if path == "-" {
		return "stdin"
	}
	return path
}

// readInput reads path, or stdin when path is "-".
func readInput(path string) ([]byte, error) {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading stdin: %w", err)
		}
		return data, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return data, nil
}

// decodeJSONFile unmarshals path (or stdin) into v, rejecting unknown fields so
// a mistyped key is reported instead of silently ignored.
func decodeJSONFile(path string, v any) error {
	data, err := readInput(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("parsing %s: %w", inputName(path), err)
	}
	return nil
}

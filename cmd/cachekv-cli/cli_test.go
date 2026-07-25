package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFlagsAnywhere(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		wantSecure     bool
		wantPositional []string
	}{
		{"flag after positional", []string{"mydb", "--secure"}, true, []string{"mydb"}},
		{"flag before positional", []string{"--secure", "mydb"}, true, []string{"mydb"}},
		{"no flag", []string{"mydb"}, false, []string{"mydb"}},
		{"flag between positionals", []string{"a", "--secure", "b"}, true, []string{"a", "b"}},
		{"nothing", nil, false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(os.Stderr)
			secure := fs.Bool("secure", false, "")

			positional, err := parseFlagsAnywhere(fs, tt.args)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPositional, positional)
			assert.Equal(t, tt.wantSecure, *secure)
		})
	}
}

func TestParseFlagsAnywhereUnknownFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(nil)
	_, err := parseFlagsAnywhere(fs, []string{"mydb", "--nope"})
	if !assert.Error(t, err) {
		return
	}

	// flagError must keep the failure a usage error, but let -h through so the
	// caller can print usage and exit 0.
	var ue usageError
	assert.ErrorAs(t, flagError(err), &ue)
	assert.ErrorIs(t, flagError(flag.ErrHelp), flag.ErrHelp)
}

func TestReadEntries(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		assert.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}

	t.Run("utf8 and base64 values", func(t *testing.T) {
		path := write("good.json", `[
			{"key":"alpha","value":"one"},
			{"key":"beta","value":"AQIDBA==","encoding":"base64"},
			{"key":"gamma","value":"three","encoding":"utf8"}
		]`)

		entries, err := readEntries(path)
		if !assert.NoError(t, err) || !assert.Len(t, entries, 3) {
			return
		}
		assert.Equal(t, "alpha", entries[0].GetKey())
		assert.Equal(t, []byte("one"), entries[0].GetValue())
		assert.Equal(t, []byte{1, 2, 3, 4}, entries[1].GetValue())
		assert.Equal(t, []byte("three"), entries[2].GetValue())
	})

	t.Run("rejects bad input", func(t *testing.T) {
		cases := map[string]string{
			"empty array":    `[]`,
			"empty key":      `[{"key":"","value":"x"}]`,
			"duplicate key":  `[{"key":"a","value":"1"},{"key":"a","value":"2"}]`,
			"bad encoding":   `[{"key":"a","value":"1","encoding":"rot13"}]`,
			"bad base64":     `[{"key":"a","value":"!!!","encoding":"base64"}]`,
			"unknown field":  `[{"key":"a","valu":"1"}]`,
			"not an array":   `{"key":"a","value":"1"}`,
			"malformed json": `[{"key":`,
		}
		for name, body := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := readEntries(write(name+".json", body))
				assert.Error(t, err)
			})
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := readEntries(filepath.Join(dir, "absent.json"))
		assert.Error(t, err)
	})
}

func TestApplyConfigPatchOnlyTouchesPresentFields(t *testing.T) {
	current := configResult{
		StorePath:   "/var/lib/cachekv",
		SecureNewDb: true,
		MetaStore:   "/var/lib/cachekv/meta",
		MetaFile:    "meta.db",
	}

	t.Run("empty patch is a no-op", func(t *testing.T) {
		assert.Equal(t, current, applyConfigPatch(current, configPatch{}))
	})

	t.Run("present fields overwrite, including zero values", func(t *testing.T) {
		newPath := "/srv/cachekv"
		secure := false
		updated := applyConfigPatch(current, configPatch{StorePath: &newPath, SecureNewDb: &secure})

		assert.Equal(t, "/srv/cachekv", updated.StorePath)
		assert.False(t, updated.SecureNewDb)
		// Untouched fields keep the server's values rather than being blanked.
		assert.Equal(t, current.MetaStore, updated.MetaStore)
		assert.Equal(t, current.MetaFile, updated.MetaFile)
	})
}

func TestValueRendering(t *testing.T) {
	binary := []byte{0xff, 0xfe, 0x00}

	t.Run("json result tags its encoding", func(t *testing.T) {
		text := newEntryResult("k", []byte("hello"))
		assert.Equal(t, entryResult{Key: "k", Value: "hello", Encoding: "utf8"}, text)

		bin := newEntryResult("k", binary)
		assert.Equal(t, "base64", bin.Encoding)
		assert.Equal(t, "//4A", bin.Value)
	})

	t.Run("text output stays on one line", func(t *testing.T) {
		assert.Equal(t, "hello", textValue([]byte("hello")))
		assert.Equal(t, "base64://4A", textValue(binary))
		// Tabs and newlines would break the key\tvalue line format.
		assert.Equal(t, "base64:YQli", textValue([]byte{'a', '\t', 'b'}))
		assert.Equal(t, "base64:YQpi", textValue([]byte{'a', '\n', 'b'}))
	})
}

func TestEnvOr(t *testing.T) {
	const key = "CACHEKV_CLI_TEST_ADDR"
	assert.Equal(t, "fallback", envOr(key, "fallback"))

	t.Setenv(key, "host:1234")
	assert.Equal(t, "host:1234", envOr(key, "fallback"))
}

func TestLookup(t *testing.T) {
	for _, c := range commands {
		found := lookup(c.name)
		if !assert.NotNil(t, found, "command %q must be resolvable", c.name) {
			continue
		}
		assert.Equal(t, c.name, found.name)
		assert.NotEmpty(t, found.usage)
		assert.NotEmpty(t, found.summary)
		assert.NotNil(t, found.run)
	}
	assert.Nil(t, lookup("no-such-command"))
}

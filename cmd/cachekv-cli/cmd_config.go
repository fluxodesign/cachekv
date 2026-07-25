package main

import (
	"fmt"
	"io"
	"text/tabwriter"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

// configResult mirrors cachekv.Config's JSON field names, so `config get`
// output can be edited and fed straight back to `config set`.
type configResult struct {
	StorePath   string `json:"store_path"`
	SecureNewDb bool   `json:"secure_new_db"`
	MetaStore   string `json:"meta_store"`
	MetaFile    string `json:"meta_file"`
}

// configPatch is the `config set` input. Fields are pointers so an omitted key
// keeps the server's current value instead of resetting it to the zero value —
// UpdateConfigurations replaces the whole config, so a partial file would
// otherwise blank out the fields it did not mention.
type configPatch struct {
	StorePath   *string `json:"store_path"`
	SecureNewDb *bool   `json:"secure_new_db"`
	MetaStore   *string `json:"meta_store"`
	MetaFile    *string `json:"meta_file"`
}

func cmdConfig(e *env, args []string) error {
	if len(args) == 0 {
		return usagef("config needs a subcommand")
	}
	switch args[0] {
	case "get":
		return configGet(e, args[1:])
	case "set":
		return configSet(e, args[1:])
	default:
		return usagef("unknown config subcommand %q", args[0])
	}
}

func configGet(e *env, args []string) error {
	if len(args) != 0 {
		return usagef("config get takes no arguments")
	}

	config, err := fetchConfig(e)
	if err != nil {
		return err
	}
	return e.emit(config, func(w io.Writer) { printConfig(w, config) })
}

func configSet(e *env, args []string) error {
	if len(args) != 1 {
		return usagef(`config set needs a JSON file (or "-" for stdin), got %d arguments`, len(args))
	}

	var patch configPatch
	if err := decodeJSONFile(args[0], &patch); err != nil {
		return err
	}

	current, err := fetchConfig(e)
	if err != nil {
		return err
	}
	updated := applyConfigPatch(current, patch)

	ctx, cancel := e.ctx()
	defer cancel()
	req := &cachekvv1.UpdateConfigurationsRequest{Config: &cachekvv1.ConfigMessage{
		StorePath:   updated.StorePath,
		SecureNewDb: updated.SecureNewDb,
		MetaStore:   updated.MetaStore,
		MetaFile:    updated.MetaFile,
	}}
	if _, err := e.client.UpdateConfigurations(ctx, req); err != nil {
		return err
	}

	return e.emit(updated, func(w io.Writer) {
		fmt.Fprintln(w, "configuration updated")
		printConfig(w, updated)
	})
}

func fetchConfig(e *env) (configResult, error) {
	ctx, cancel := e.ctx()
	defer cancel()
	resp, err := e.client.ListConfigurations(ctx, &cachekvv1.ListConfigurationsRequest{})
	if err != nil {
		return configResult{}, err
	}
	config := resp.GetConfig()
	return configResult{
		StorePath:   config.GetStorePath(),
		SecureNewDb: config.GetSecureNewDb(),
		MetaStore:   config.GetMetaStore(),
		MetaFile:    config.GetMetaFile(),
	}, nil
}

func applyConfigPatch(config configResult, patch configPatch) configResult {
	if patch.StorePath != nil {
		config.StorePath = *patch.StorePath
	}
	if patch.SecureNewDb != nil {
		config.SecureNewDb = *patch.SecureNewDb
	}
	if patch.MetaStore != nil {
		config.MetaStore = *patch.MetaStore
	}
	if patch.MetaFile != nil {
		config.MetaFile = *patch.MetaFile
	}
	return config
}

func printConfig(w io.Writer, config configResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "store_path:\t%s\n", config.StorePath)
	fmt.Fprintf(tw, "secure_new_db:\t%t\n", config.SecureNewDb)
	fmt.Fprintf(tw, "meta_store:\t%s\n", config.MetaStore)
	fmt.Fprintf(tw, "meta_file:\t%s\n", config.MetaFile)
	_ = tw.Flush()
}

// Package grpcserver implements the CacheKV gRPC service on top of the
// cachekv package API.
package grpcserver

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/fluxodesign/cachekv/cachekv"
	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

// metaDbKeyPrefix is the prefix cachekv puts on every database's meta-database
// key. ListDatabases returns those raw keys, but every other RPC takes a bare
// database name, so the prefix is stripped on the way out to keep the two
// consistent. It must match cachekv's unexported prefixMetaDb.
const metaDbKeyPrefix = "fxstorage_db:"

// Server implements cachekvv1.CacheKVServer, adapting each RPC to the
// equivalent cachekv library call.
type Server struct {
	cachekvv1.UnimplementedCacheKVServer
}

// New returns a Server ready to be registered with a grpc.Server.
func New() *Server {
	return &Server{}
}

// CreateDatabase creates a new (optionally encrypted) database.
func (s *Server) CreateDatabase(_ context.Context, req *cachekvv1.CreateDatabaseRequest) (*cachekvv1.CreateDatabaseResponse, error) {
	if err := checkDbName(req.GetDbName()); err != nil {
		return nil, err
	}
	if err := cachekv.CreateDatabase(req.GetDbName(), req.GetSecure()); err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.CreateDatabaseResponse{}, nil
}

// InsertEntry writes a single key/value pair.
func (s *Server) InsertEntry(_ context.Context, req *cachekvv1.InsertEntryRequest) (*cachekvv1.InsertEntryResponse, error) {
	if err := checkDbNameAndKey(req.GetDbName(), req.GetKey()); err != nil {
		return nil, err
	}
	if err := cachekv.InsertEntry(req.GetDbName(), req.GetKey(), req.GetValue()); err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.InsertEntryResponse{}, nil
}

// UpdateEntry overwrites the value at a key. Writes are upserts, so this
// succeeds whether or not the key already exists.
func (s *Server) UpdateEntry(_ context.Context, req *cachekvv1.UpdateEntryRequest) (*cachekvv1.UpdateEntryResponse, error) {
	if err := checkDbNameAndKey(req.GetDbName(), req.GetKey()); err != nil {
		return nil, err
	}
	if err := cachekv.UpdateEntry(req.GetDbName(), req.GetKey(), req.GetValue()); err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.UpdateEntryResponse{}, nil
}

// RemoveEntry deletes a key. Deleting a key that does not exist is not an
// error, matching the underlying store's semantics.
func (s *Server) RemoveEntry(_ context.Context, req *cachekvv1.RemoveEntryRequest) (*cachekvv1.RemoveEntryResponse, error) {
	if err := checkDbNameAndKey(req.GetDbName(), req.GetKey()); err != nil {
		return nil, err
	}
	if err := cachekv.RemoveEntry(req.GetDbName(), req.GetKey()); err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.RemoveEntryResponse{}, nil
}

// BatchInsert writes every entry in the request as one all-or-nothing batch.
func (s *Server) BatchInsert(_ context.Context, req *cachekvv1.BatchInsertRequest) (*cachekvv1.BatchInsertResponse, error) {
	if err := checkDbName(req.GetDbName()); err != nil {
		return nil, err
	}

	protoEntries := req.GetEntries()
	entries := make(map[string][]byte, len(protoEntries))
	for i, entry := range protoEntries {
		key := entry.GetKey()
		if key == "" {
			return nil, status.Errorf(codes.InvalidArgument, "entry %d has an empty key", i)
		}
		// The batch is a map, so a duplicate key would silently drop one of the
		// two values. Reject it rather than pick a winner.
		if _, dup := entries[key]; dup {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate key %q at entry %d", key, i)
		}
		entries[key] = entry.GetValue()
	}

	if len(entries) > 0 {
		if err := cachekv.BatchInsert(req.GetDbName(), entries); err != nil {
			return nil, toStatus(err)
		}
	}
	return &cachekvv1.BatchInsertResponse{Count: int32(len(entries))}, nil
}

// GetEntry reads the value stored at a key.
func (s *Server) GetEntry(_ context.Context, req *cachekvv1.GetEntryRequest) (*cachekvv1.GetEntryResponse, error) {
	if err := checkDbNameAndKey(req.GetDbName(), req.GetKey()); err != nil {
		return nil, err
	}
	value, err := cachekv.GetEntry(req.GetDbName(), req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.GetEntryResponse{Value: value}, nil
}

// GetAll streams every entry in a database, one message per entry. The
// underlying (*Storage).All buffers the whole database into a map first; the
// streaming is about staying under gRPC's message size cap and letting clients
// consume incrementally, not about bounding server memory.
func (s *Server) GetAll(req *cachekvv1.GetAllRequest, stream grpc.ServerStreamingServer[cachekvv1.Entry]) error {
	if err := checkDbName(req.GetDbName()); err != nil {
		return err
	}

	store, err := cachekv.GetStorageObject(req.GetDbName())
	if err != nil {
		return toStatus(err)
	}
	// Release the pool reference even if the client disconnects mid-stream.
	defer store.Close()

	entries, err := store.All()
	if err != nil {
		return toStatus(err)
	}

	ctx := stream.Context()
	for key, value := range entries {
		if err := ctx.Err(); err != nil {
			return toStatus(err)
		}
		if err := stream.Send(&cachekvv1.Entry{Key: key, Value: value}); err != nil {
			return err
		}
	}
	return nil
}

// ListDatabases returns the names of all known databases.
func (s *Server) ListDatabases(_ context.Context, _ *cachekvv1.ListDatabasesRequest) (*cachekvv1.ListDatabasesResponse, error) {
	keys, err := cachekv.ListDatabases()
	if err != nil {
		return nil, toStatus(err)
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, strings.TrimPrefix(key, metaDbKeyPrefix))
	}
	return &cachekvv1.ListDatabasesResponse{Names: names}, nil
}

// ListConfigurations returns the configuration persisted in the meta database.
func (s *Server) ListConfigurations(_ context.Context, _ *cachekvv1.ListConfigurationsRequest) (*cachekvv1.ListConfigurationsResponse, error) {
	config, err := cachekv.ListConfigurations()
	if err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.ListConfigurationsResponse{Config: configToProto(config)}, nil
}

// UpdateConfigurations replaces the persisted configuration wholesale. Callers
// wanting a partial update must read-modify-write.
func (s *Server) UpdateConfigurations(_ context.Context, req *cachekvv1.UpdateConfigurationsRequest) (*cachekvv1.UpdateConfigurationsResponse, error) {
	if req.GetConfig() == nil {
		return nil, status.Error(codes.InvalidArgument, "config is required")
	}
	if err := cachekv.UpdateConfigurations(configFromProto(req.GetConfig())); err != nil {
		return nil, toStatus(err)
	}
	return &cachekvv1.UpdateConfigurationsResponse{}, nil
}

// GetMetrics returns a snapshot of the process's operation counters and
// latency percentiles.
func (s *Server) GetMetrics(_ context.Context, _ *cachekvv1.GetMetricsRequest) (*cachekvv1.GetMetricsResponse, error) {
	collector, ok := cachekv.GetMetricsCollector().(*cachekv.SimpleMetricsCollector)
	if !ok {
		return nil, status.Error(codes.Unavailable, "metrics collection is not enabled")
	}
	m := collector.GetMetrics()
	return &cachekvv1.GetMetricsResponse{
		TotalOperations: m.TotalOperations,
		Created:         m.Created,
		Reads:           m.Reads,
		Writes:          m.Writes,
		Deletes:         m.Deletes,
		Errors:          m.Errors,
		DatabasesActive: m.DatabasesActive,
		MeanLatency:     durationpb.New(m.MeanLatency),
		P50Latency:      durationpb.New(m.P50Latency),
		P95Latency:      durationpb.New(m.P95Latency),
		P99Latency:      durationpb.New(m.P99Latency),
		Uptime:          durationpb.New(m.Uptime),
	}, nil
}

// Ping is a trivial liveness check independent of the standard gRPC health
// service registered alongside it.
func (s *Server) Ping(_ context.Context, _ *cachekvv1.PingRequest) (*cachekvv1.PingResponse, error) {
	return &cachekvv1.PingResponse{}, nil
}

// checkDbName rejects an empty database name up front. Without it the lookup
// would fail as a meta-key miss and surface as NotFound, which reads as "no
// such database" rather than "you forgot an argument".
func checkDbName(dbName string) error {
	if dbName == "" {
		return status.Error(codes.InvalidArgument, "db_name is required")
	}
	return nil
}

func checkDbNameAndKey(dbName, key string) error {
	if err := checkDbName(dbName); err != nil {
		return err
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	return nil
}

func configToProto(config *cachekv.Config) *cachekvv1.ConfigMessage {
	if config == nil {
		return nil
	}
	return &cachekvv1.ConfigMessage{
		StorePath:   config.StorePath,
		SecureNewDb: config.SecureNewDb,
		MetaStore:   config.MetaStore,
		MetaFile:    config.MetaFile,
	}
}

func configFromProto(msg *cachekvv1.ConfigMessage) *cachekv.Config {
	return &cachekv.Config{
		StorePath:   msg.GetStorePath(),
		SecureNewDb: msg.GetSecureNewDb(),
		MetaStore:   msg.GetMetaStore(),
		MetaFile:    msg.GetMetaFile(),
	}
}

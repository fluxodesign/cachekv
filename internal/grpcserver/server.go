// Package grpcserver implements the CacheKV gRPC service on top of the
// cachekv package API.
package grpcserver

import (
	"context"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

// Server implements cachekvv1.CacheKVServer. Only Ping is implemented in this
// stage; every other RPC returns codes.Unimplemented via the embedded
// UnimplementedCacheKVServer until the library calls are wired up.
type Server struct {
	cachekvv1.UnimplementedCacheKVServer
}

// New returns a Server ready to be registered with a grpc.Server.
func New() *Server {
	return &Server{}
}

// Ping is a trivial liveness check independent of the standard gRPC health
// service registered alongside it.
func (s *Server) Ping(_ context.Context, _ *cachekvv1.PingRequest) (*cachekvv1.PingResponse, error) {
	return &cachekvv1.PingResponse{}, nil
}

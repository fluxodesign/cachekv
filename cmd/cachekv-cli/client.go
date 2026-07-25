package main

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

// dial prepares a plaintext, unauthenticated connection to cachekv-server —
// the server ships without TLS or auth, so there is nothing to negotiate. The
// connection is lazy: the first RPC is what actually reaches the network, so a
// wrong address surfaces as an Unavailable status from that RPC rather than an
// error here.
func dial(addr string) (cachekvv1.CacheKVClient, io.Closer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return cachekvv1.NewCacheKVClient(conn), conn, nil
}

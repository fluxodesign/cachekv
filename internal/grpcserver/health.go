package grpcserver

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// cacheKVServiceName must match the ServiceName registered by
// cachekvv1.CacheKV_ServiceDesc ("<package>.<service>").
const cacheKVServiceName = "cachekv.v1.CacheKV"

// NewHealthServer returns a health.Server reporting SERVING for both the
// overall server ("") and the CacheKV service.
func NewHealthServer() *health.Server {
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	h.SetServingStatus(cacheKVServiceName, healthpb.HealthCheckResponse_SERVING)
	return h
}

// SetNotServing marks the server as NOT_SERVING so health-check clients stop
// routing new traffic while in-flight RPCs continue to drain via
// grpc.Server.GracefulStop.
func SetNotServing(h *health.Server) {
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	h.SetServingStatus(cacheKVServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
}

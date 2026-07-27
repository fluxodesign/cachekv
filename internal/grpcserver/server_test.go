package grpcserver

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/fluxodesign/cachekv/cachekv"
	cachekvv1 "github.com/fluxodesign/cachekv/gen/cachekv/v1"
)

const bufSize = 1024 * 1024

// cachekv keeps its store, keyring and metrics in package-level globals, and
// Startup/Shutdown are not re-entrant from outside the package (the connection
// pool's once cannot be reset). So the whole package shares one server and one
// store, brought up here; individual tests must stay sequential and use
// distinct database names.
var (
	testClient cachekvv1.CacheKVClient
	testHealth healthpb.HealthClient
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests is split out of TestMain so the deferred cleanup still runs — the
// os.Exit that TestMain has to make would otherwise skip it.
func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "cachekv-grpcserver-")
	if err != nil {
		log.Fatalf("creating temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Startup only initialises a fresh store when StorePath does not exist yet,
	// so point it at a subdirectory of the temp dir rather than the temp dir
	// itself.
	cachekv.StorePath = filepath.Join(dir, "store") + string(os.PathSeparator)
	cachekv.KeyPath = filepath.Join(dir, "private")
	if err := os.Setenv("POOL_TIMEOUT_MS", "100"); err != nil {
		log.Fatalf("setting pool timeout: %v", err)
	}
	cachekv.Startup()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := cachekv.Shutdown(ctx, 30*time.Second); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	cachekvv1.RegisterCacheKVServer(srv, New())
	healthSrv := NewHealthServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("bufconn server stopped: %v", err)
		}
	}()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dialing bufconn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	testClient = cachekvv1.NewCacheKVClient(conn)
	testHealth = healthpb.NewHealthClient(conn)

	return m.Run()
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// createDB creates a database for a single test and returns its name.
func createDB(t *testing.T, name string, secure bool) string {
	t.Helper()
	ctx := testContext(t)
	_, err := testClient.CreateDatabase(ctx, &cachekvv1.CreateDatabaseRequest{DbName: name, Secure: secure})
	assert.NoError(t, err)
	return name
}

func TestPing(t *testing.T) {
	resp, err := testClient.Ping(testContext(t), &cachekvv1.PingRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestHealthServing(t *testing.T) {
	resp, err := testHealth.Check(testContext(t), &healthpb.HealthCheckRequest{})
	assert.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	resp, err = testHealth.Check(testContext(t), &healthpb.HealthCheckRequest{Service: cacheKVServiceName})
	assert.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestCreateDatabaseAndList(t *testing.T) {
	name := createDB(t, "listed-db", false)

	resp, err := testClient.ListDatabases(testContext(t), &cachekvv1.ListDatabasesRequest{})
	assert.NoError(t, err)
	// Names come back bare, not as raw "fxstorage_db:" meta keys, so they can be
	// fed straight back into the other RPCs.
	assert.Contains(t, resp.GetNames(), name)
	for _, got := range resp.GetNames() {
		assert.NotContains(t, got, metaDbKeyPrefix)
	}
}

func TestCreateDatabaseAlreadyExists(t *testing.T) {
	name := createDB(t, "duplicate-db", false)

	_, err := testClient.CreateDatabase(testContext(t), &cachekvv1.CreateDatabaseRequest{DbName: name})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestCreateDatabaseEmptyName(t *testing.T) {
	_, err := testClient.CreateDatabase(testContext(t), &cachekvv1.CreateDatabaseRequest{DbName: ""})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestInsertAndGetEntry(t *testing.T) {
	name := createDB(t, "entries-db", false)
	ctx := testContext(t)

	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{
		DbName: name, Key: "alpha", Value: []byte("one"),
	})
	assert.NoError(t, err)

	resp, err := testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name, Key: "alpha"})
	assert.NoError(t, err)
	assert.Equal(t, []byte("one"), resp.GetValue())
}

func TestInsertAndGetEntrySecureDatabase(t *testing.T) {
	name := createDB(t, "secure-entries-db", true)
	ctx := testContext(t)

	value := []byte{0x00, 0x01, 0xff, 0xfe}
	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{
		DbName: name, Key: "binary", Value: value,
	})
	assert.NoError(t, err)

	resp, err := testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name, Key: "binary"})
	assert.NoError(t, err)
	assert.Equal(t, value, resp.GetValue())
}

func TestUpdateEntry(t *testing.T) {
	name := createDB(t, "update-db", false)
	ctx := testContext(t)

	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{
		DbName: name, Key: "k", Value: []byte("before"),
	})
	assert.NoError(t, err)

	_, err = testClient.UpdateEntry(ctx, &cachekvv1.UpdateEntryRequest{
		DbName: name, Key: "k", Value: []byte("after"),
	})
	assert.NoError(t, err)

	resp, err := testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name, Key: "k"})
	assert.NoError(t, err)
	assert.Equal(t, []byte("after"), resp.GetValue())
}

func TestRemoveEntry(t *testing.T) {
	name := createDB(t, "remove-db", false)
	ctx := testContext(t)

	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{
		DbName: name, Key: "doomed", Value: []byte("v"),
	})
	assert.NoError(t, err)

	_, err = testClient.RemoveEntry(ctx, &cachekvv1.RemoveEntryRequest{DbName: name, Key: "doomed"})
	assert.NoError(t, err)

	_, err = testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name, Key: "doomed"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetEntryUnknownKey(t *testing.T) {
	name := createDB(t, "missing-key-db", false)

	_, err := testClient.GetEntry(testContext(t), &cachekvv1.GetEntryRequest{DbName: name, Key: "nope"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetEntryUnknownDatabase(t *testing.T) {
	_, err := testClient.GetEntry(testContext(t), &cachekvv1.GetEntryRequest{
		DbName: "no-such-db", Key: "k",
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestEntryRPCsRejectEmptyKey(t *testing.T) {
	name := createDB(t, "empty-key-db", false)
	ctx := testContext(t)

	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{DbName: name, Value: []byte("v")})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = testClient.UpdateEntry(ctx, &cachekvv1.UpdateEntryRequest{DbName: name, Value: []byte("v")})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = testClient.RemoveEntry(ctx, &cachekvv1.RemoveEntryRequest{DbName: name})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestBatchInsertAndGetAll(t *testing.T) {
	name := createDB(t, "batch-db", false)
	ctx := testContext(t)

	want := map[string][]byte{
		"one":   []byte("1"),
		"two":   []byte("2"),
		"three": []byte("3"),
	}
	entries := make([]*cachekvv1.Entry, 0, len(want))
	for key, value := range want {
		entries = append(entries, &cachekvv1.Entry{Key: key, Value: value})
	}

	resp, err := testClient.BatchInsert(ctx, &cachekvv1.BatchInsertRequest{DbName: name, Entries: entries})
	assert.NoError(t, err)
	assert.Equal(t, int32(len(want)), resp.GetCount())

	assert.Equal(t, want, drainGetAll(t, ctx, name))
}

func TestBatchInsertDuplicateKey(t *testing.T) {
	name := createDB(t, "batch-dup-db", false)

	_, err := testClient.BatchInsert(testContext(t), &cachekvv1.BatchInsertRequest{
		DbName: name,
		Entries: []*cachekvv1.Entry{
			{Key: "same", Value: []byte("a")},
			{Key: "same", Value: []byte("b")},
		},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestBatchInsertEmptyKey(t *testing.T) {
	name := createDB(t, "batch-empty-key-db", false)

	_, err := testClient.BatchInsert(testContext(t), &cachekvv1.BatchInsertRequest{
		DbName:  name,
		Entries: []*cachekvv1.Entry{{Value: []byte("a")}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetAllEmptyDatabase(t *testing.T) {
	name := createDB(t, "empty-db", false)

	assert.Empty(t, drainGetAll(t, testContext(t), name))
}

func TestGetAllUnknownDatabase(t *testing.T) {
	stream, err := testClient.GetAll(testContext(t), &cachekvv1.GetAllRequest{DbName: "no-such-db"})
	assert.NoError(t, err)
	_, err = stream.Recv()
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestConfigurationRoundTrip(t *testing.T) {
	ctx := testContext(t)

	resp, err := testClient.ListConfigurations(ctx, &cachekvv1.ListConfigurationsRequest{})
	assert.NoError(t, err)
	original := resp.GetConfig()
	assert.NotNil(t, original)
	assert.Equal(t, cachekv.StorePath, original.GetStorePath())

	// Restore whatever was there before, so later tests see the store the way
	// Startup left it.
	t.Cleanup(func() {
		_, err := testClient.UpdateConfigurations(context.Background(),
			&cachekvv1.UpdateConfigurationsRequest{Config: original})
		assert.NoError(t, err)
	})

	updated := &cachekvv1.ConfigMessage{
		StorePath:   original.GetStorePath(),
		SecureNewDb: !original.GetSecureNewDb(),
		MetaStore:   original.GetMetaStore(),
		MetaFile:    original.GetMetaFile(),
	}
	_, err = testClient.UpdateConfigurations(ctx, &cachekvv1.UpdateConfigurationsRequest{Config: updated})
	assert.NoError(t, err)

	resp, err = testClient.ListConfigurations(ctx, &cachekvv1.ListConfigurationsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, !original.GetSecureNewDb(), resp.GetConfig().GetSecureNewDb())
	assert.Equal(t, original.GetStorePath(), resp.GetConfig().GetStorePath())
}

func TestUpdateConfigurationsRequiresConfig(t *testing.T) {
	_, err := testClient.UpdateConfigurations(testContext(t), &cachekvv1.UpdateConfigurationsRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetMetrics(t *testing.T) {
	name := createDB(t, "metrics-db", false)
	ctx := testContext(t)

	_, err := testClient.InsertEntry(ctx, &cachekvv1.InsertEntryRequest{
		DbName: name, Key: "k", Value: []byte("v"),
	})
	assert.NoError(t, err)
	_, err = testClient.GetEntry(ctx, &cachekvv1.GetEntryRequest{DbName: name, Key: "k"})
	assert.NoError(t, err)

	resp, err := testClient.GetMetrics(ctx, &cachekvv1.GetMetricsRequest{})
	assert.NoError(t, err)
	assert.Positive(t, resp.GetWrites())
	assert.Positive(t, resp.GetReads())
	assert.Positive(t, resp.GetTotalOperations())
	assert.Positive(t, resp.GetUptime().AsDuration())
}

func drainGetAll(t *testing.T, ctx context.Context, dbName string) map[string][]byte {
	t.Helper()
	stream, err := testClient.GetAll(ctx, &cachekvv1.GetAllRequest{DbName: dbName})
	assert.NoError(t, err)

	got := make(map[string][]byte)
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return got
		}
		if !assert.NoError(t, err) {
			return got
		}
		got[entry.GetKey()] = entry.GetValue()
	}
}

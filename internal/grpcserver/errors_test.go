package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fluxodesign/cachekv/cachekv"
)

func TestToStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"validation error", &cachekv.ValidationError{Field: "StorePath", Code: 5001, Message: "bad"}, codes.InvalidArgument},
		{
			"wrapped validation error",
			fmt.Errorf("startup: %w", &cachekv.ValidationError{Field: "KeyPath", Code: 5002, Message: "bad"}),
			codes.InvalidArgument,
		},
		{
			"meta key not found",
			&cachekv.EMetaKeyNotFound{Code: 8404, Message: "meta key not found", Wrapped: badger.ErrKeyNotFound},
			codes.NotFound,
		},
		{"badger key not found", badger.ErrKeyNotFound, codes.NotFound},
		{"missing file", fmt.Errorf("stat store/db: %w", fs.ErrNotExist), codes.NotFound},
		{"empty key", badger.ErrEmptyKey, codes.InvalidArgument},
		{"reserved key prefix", badger.ErrInvalidKey, codes.InvalidArgument},
		{"txn conflict", badger.ErrConflict, codes.Aborted},
		{"txn too big", badger.ErrTxnTooBig, codes.ResourceExhausted},
		{"db closed", badger.ErrDBClosed, codes.Unavailable},
		{"deadline exceeded", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"canceled", context.Canceled, codes.Canceled},
		{"permission denied", fmt.Errorf("open store: %w", fs.ErrPermission), codes.PermissionDenied},

		// The library's plain-string errors, quoted verbatim from cachekv.
		{"shutting down", errors.New("system is shutting down - operation rejected"), codes.Unavailable},
		{"rotating key", errors.New("maintenance: rotating key"), codes.Unavailable},
		{"already exists", errors.New("database already exists"), codes.AlreadyExists},
		{"inactive db", errors.New("mydb - error: trying to access inactive db"), codes.FailedPrecondition},
		{"key mismatch", errors.New("encryption key mismatch for already open database"), codes.FailedPrecondition},

		{"unrecognised", errors.New("something went sideways"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toStatus(tt.err)
			assert.Equal(t, tt.want, status.Code(got))
			assert.Equal(t, tt.err.Error(), status.Convert(got).Message())
		})
	}
}

func TestToStatusNil(t *testing.T) {
	assert.Nil(t, toStatus(nil))
}

func TestToStatusPassesThroughExistingStatus(t *testing.T) {
	original := status.Error(codes.InvalidArgument, "db_name is required")
	assert.Equal(t, original, toStatus(original))
}

// "already exists" must not win over a typed error that says otherwise: the
// substring fallbacks only run once every errors.As/errors.Is check has missed.
func TestToStatusTypedErrorBeatsSubstring(t *testing.T) {
	err := &cachekv.ValidationError{Field: "db", Code: 1, Message: "database already exists"}
	assert.Equal(t, codes.InvalidArgument, status.Code(toStatus(err)))
}

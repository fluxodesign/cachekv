package grpcserver

import (
	"context"
	"errors"
	"io/fs"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fluxodesign/cachekv/cachekv"
)

// The cachekv library returns most of its failures as plain strings built from
// unexported constants, so there is nothing to match on with errors.Is. These
// are the substrings of those messages that carry a code more useful than
// Internal. Brittle by construction — see the plan's "Decisions" section; the
// typed errors above them are matched properly and take precedence.
const (
	msgShuttingDown = "shutting down"
	msgRotatingKey  = "rotating key"
	msgAlreadyExist = "already exists"
	msgInactiveDb   = "inactive db"
	msgKeyMismatch  = "encryption key mismatch"
)

// toStatus converts a cachekv (or badger) error into a gRPC status error.
// It returns nil for a nil error and passes through errors that already carry
// a status, so handlers can mix their own status.Error calls with library
// errors and funnel everything through one call.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}

	var validation *cachekv.ValidationError
	if errors.As(err, &validation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var metaKeyNotFound *cachekv.EMetaKeyNotFound
	if errors.As(err, &metaKeyNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}

	switch {
	case errors.Is(err, badger.ErrKeyNotFound), errors.Is(err, fs.ErrNotExist):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, badger.ErrEmptyKey), errors.Is(err, badger.ErrInvalidKey),
		errors.Is(err, badger.ErrBannedKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, badger.ErrConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, badger.ErrTxnTooBig):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, badger.ErrDBClosed), errors.Is(err, badger.ErrBlockedWrites):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, fs.ErrPermission):
		return status.Error(codes.PermissionDenied, err.Error())
	}

	// Substring fallbacks for the library's plain-string errors.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, msgShuttingDown), strings.Contains(msg, msgRotatingKey):
		return status.Error(codes.Unavailable, err.Error())
	case strings.Contains(msg, msgAlreadyExist):
		return status.Error(codes.AlreadyExists, err.Error())
	case strings.Contains(msg, msgInactiveDb):
		return status.Error(codes.FailedPrecondition, err.Error())
	case strings.Contains(msg, msgKeyMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}

package cachekv

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ValidateConfiguration performs comprehensive validation of system configuration and environment
func ValidateConfiguration(ctx context.Context, config *Config, strict bool) error {
	var errors []error

	log.Println("[1/6] Validating store path...")
	if err := validateStorePath(config.StorePath); err != nil {
		errors = append(errors, err)
		if strict {
			return &ValidationError{Field: "Configuration", Code: 5001, Message: "store path validation failed"}
		}
		log.Printf("Warning: %v\n", err)
	}

	log.Println("[2/6] Validating key storage...")
	if err := validateKeyPath(KeyPath); err != nil {
		errors = append(errors, err)
		if strict {
			return &ValidationError{Field: "Configuration", Code: 5002, Message: "key path validation failed"}
		}
		log.Printf("Warning: %v\n", err)
	}

	log.Println("[3/6] Validating permissions...")
	if err := validatePermissions(config.StorePath); err != nil {
		errors = append(errors, err)
		if strict {
			return &ValidationError{Field: "Configuration", Code: 5003, Message: "permission validation failed"}
		}
		log.Printf("Warning: %v\n", err)
	}

	if err := validatePermissions(KeyPath); err != nil {
		errors = append(errors, err)
		if strict {
			return &ValidationError{Field: "Configuration", Code: 5004, Message: "key permission validation failed"}
		}
		log.Printf("Warning: %v\n", err)
	}

	log.Println("[4/6] Validating dependencies...")
	if err := validateDependencies(); err != nil {
		errors = append(errors, err)
		return &ValidationError{Field: "Configuration", Code: 5005, Message: "dependency validation failed"}
	}

	log.Println("[5/6] Validating encryption capability...")
	if err := validateEncryptionCapability(); err != nil {
		errors = append(errors, err)
		return &ValidationError{Field: "Configuration", Code: 5006, Message: "encryption validation failed"}
	}

	log.Println("[6/6] Validating GC configuration...")
	if err := validateGCConfig(); err != nil {
		errors = append(errors, err)
		if strict {
			return &ValidationError{Field: "Configuration", Code: 5007, Message: "GC config validation failed"}
		}
		log.Printf("Warning: %v\n", err)
	}

	// Additional configuration-specific validations
	if config.StorePath == "" {
		errors = append(errors, &ValidationError{Field: "StorePath", Code: 5010, Message: "store path is empty"})
	}

	if len(config.MetaFile) == 0 && loadMetaIdent().file != "" {
		log.Printf("Warning: MetaFile in config differs from runtime value\n")
	}

	// Check disk space (minimum 1GB free recommended)
	info, err := os.Stat(config.StorePath)
	if err != nil {
		errors = append(errors, &ValidationError{Field: "DiskSpace", Code: 5020, Message: "cannot stat store path"})
	} else {
		// Note: This is a basic check; more sophisticated disk space checking would require platform-specific code
		if !info.IsDir() {
			errors = append(errors, &ValidationError{Field: "StorePath", Code: 5021, Message: "store path is not a directory"})
		}
	}

	// Verify metaStorage and keyStorage are properly initialized
	if metaID := loadMetaIdent(); metaID.path == "" || metaID.file == "" {
		errors = append(errors, &ValidationError{Field: "MetaStorage", Code: 5030, Message: "meta storage not initialized"})
	}

	if keyStorage.path == "" || keyStorage.file == "" || len(keyStorage.key) != keyLength {
		errors = append(errors, &ValidationError{Field: "KeyStorage", Code: 5031, Message: "key storage not properly initialized"})
	}

	// Check if connection pool is accessible
	pool := GetConnectionPool()
	if pool == nil {
		errors = append(errors, &ValidationError{Field: "ConnectionPool", Code: 5040, Message: "connection pool not available"})
	} else {
		log.Println("Connection pool initialized successfully")
	}

	// Log validation results
	if len(errors) > 0 {
		log.Printf("Validation completed with %d warning(s)\n", len(errors))
		return &ValidationError{Field: "Configuration", Code: 5999, Message: "validation warnings detected"}
	}

	log.Println("All validations passed successfully")
	return nil
}

// validateStorePath validates that the store path exists and is accessible
func validateStorePath(storePath string) error {
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		return &ValidationError{Field: "StorePath", Code: 4096, Message: "store path does not exist"}
	}

	info, _ := os.Stat(storePath)
	if !info.IsDir() {
		return &ValidationError{Field: "StorePath", Code: 4097, Message: "store path is not a directory"}
	}

	return nil
}

// validateKeyPath ensures key storage location is valid and secure
func validateKeyPath(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		parentDir := filepath.Dir(path)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			return &ValidationError{Field: "KeyPath", Code: 4103, Message: "key storage parent directory does not exist"}
		}
	}

	info, _ := os.Stat(path)
	if info != nil && !info.IsDir() {
		return &ValidationError{Field: "KeyPath", Code: 4104, Message: "key path is not a directory"}
	}

	return nil
}

// validatePermissions ensures directories have correct permissions
func validatePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	mode := info.Mode()
	if mode.Perm().String() == "777" || mode.Perm().String() == "0777" {
		log.Printf("Warning: path %s has overly permissive permissions (777)\n", path)
	}

	return nil
}

// validateDependencies ensures required system dependencies are available
func validateDependencies() error {
	testPath := filepath.Join(os.TempDir(), "cachekv_test_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	defer os.RemoveAll(testPath)

	opt := badger.DefaultOptions(testPath)
	db, err := badger.Open(opt)
	if err != nil {
		return &ValidationError{Field: "Dependencies", Code: 4200, Message: "Badger DB initialization failed"}
	}
	defer db.Close()

	return nil
}

// validateEncryptionCapability ensures encryption functions work correctly
func validateEncryptionCapability() error {
	if _, err := os.Stat(filepath.Join(KeyPath, privateFile)); os.IsNotExist(err) {
		return &ValidationError{Field: "Encryption", Code: 4300, Message: "private key file not found"}
	}

	testMessage := []byte("test_encryption")
	encrypted, err := encryptMessage(testMessage, nil)
	if err != nil {
		return &ValidationError{Field: "Encryption", Code: 4301, Message: "encryption failed"}
	}

	decrypted, err := decryptMessage(encrypted, nil)
	if err != nil {
		return &ValidationError{Field: "Encryption", Code: 4302, Message: "decryption failed"}
	}

	if string(decrypted) != string(testMessage) {
		return &ValidationError{Field: "Encryption", Code: 4303, Message: "decrypted message does not match original"}
	}

	return nil
}

// validateGCConfig checks if Go runtime GC settings are appropriate
func validateGCConfig() error {
	gcPercent := debug.SetGCPercent(-1)
	if gcPercent < 20 || gcPercent > 100 {
		log.Printf("Warning: GC percent is %d, recommended range is 20-100\n", gcPercent)
	}
	return nil
}

// ... existing code ...

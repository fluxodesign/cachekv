// Package cachekv is an encrypted key-value store built on top of BadgerDB.
//
// It manages a set of named databases (secure or plain), a keyring that holds
// each secure database's encryption key, and a meta database that records the
// catalogue of databases, configuration, and an audit event log. Connections
// are shared through a reference-counted connection pool (see ConnectionPool).
//
// Call Startup once to initialize or open the store, then use CreateDatabase,
// InsertEntry, GetEntry, and friends to work with individual databases. Call
// Shutdown for a graceful teardown.
package cachekv

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"
)

// StorePath is the directory under which all databases (including the meta DB)
// are created, and KeyPath is the directory holding the keyring. Both may be
// overridden before calling Startup.
var (
	StorePath = "./store/"
	KeyPath   = "./.private"
)

const (
	keyLength    = 32
	fileKey      = 8
	fileIdLength = 16
	service      = "fxstorage"
)

// Startup initializes the storage system. On first run (StorePath does not yet
// exist) it creates the store directory and initializes the keyring and meta
// databases; otherwise it opens the existing ones. It then validates the
// configuration and records a startup event. Startup must be called before any
// other operation. Fatal errors during initialization terminate the process.
func Startup() {
	ctx := context.Background()
	atomic.StoreInt32(&shutdownFlag, shutdownFlagDefault)

	// Initialize metrics collector first for startup monitoring
	collectorMu.Lock()
	metricsCollector = NewSimpleMetricsCollector()
	collectorMu.Unlock()
	startupStart := time.Now()

	log.Println("Starting cachekv storage system...")
	_, err := os.Stat(StorePath)
	if err != nil && os.IsNotExist(err) {
		syscall.Umask(0)
		err = os.MkdirAll(StorePath, 0744)
		if err != nil {
			log.Fatal("error creating store dir: ", err)
			return
		}

		globalStateMu.Lock()
		defer globalStateMu.Unlock()

		err = initKeyDb()
		if err != nil {
			log.Fatal("error initializing keydb: ", err)
			return
		}
		err = initMetaDb()
		if err != nil {
			log.Fatal("error initializing meta db: ", err)
			return
		}
	} else {
		globalStateMu.Lock()
		defer globalStateMu.Unlock()

		// load up the key db
		err = openKeyDb()
		if err != nil {
			log.Fatal("error opening keydb: ", err)
			return
		}
		// load up the saved metafile
		err = openMetaDb()
		if err != nil {
			log.Fatal("error opening meta db: ", err)
			return
		}
	}

	// Perform comprehensive validation after initialization
	config := DefaultConfig()
	log.Println("Validating configuration and environment...")

	err = ValidateConfiguration(ctx, config, true) // strict=true for startup
	if err != nil {
		log.Printf("Validation warnings: %v\n", err)
	}

	startupDuration := time.Since(startupStart)
	metricsCollector.RecordLatency(ctx, "startup_init", startupDuration)

	log.Printf("Startup complete in %v\n", startupDuration)
	err = writeMetaEvent(EventTypeConfigChange, fmt.Sprintf("System started in %v", startupDuration))
	if err != nil {
		return
	}
}

// loadMetaIdent returns the current meta identity snapshot, or a zero value if the
// meta DB has not been initialised yet. Lock-free (see H2).
func loadMetaIdent() metaIdent {
	if p := metaIdentPtr.Load(); p != nil {
		return *p
	}
	return metaIdent{}
}

// storeMetaIdent atomically publishes a new meta identity. Callers set the whole
// identity at once so readers never observe a mismatched path/key.
func storeMetaIdent(dir, file string, key []byte) {
	metaIdentPtr.Store(&metaIdent{path: dir, file: file, key: key})
}

// metaPathAndKey returns the full path and encryption key of the active meta DB.
func metaPathAndKey() (string, []byte) {
	id := loadMetaIdent()
	return path.Join(id.path, id.file), id.key
}

// DefaultConfig returns a Config populated from the current store paths and the
// active meta file. It is used for validation at startup and as a baseline for
// callers that have not persisted their own configuration.
func DefaultConfig() *Config {
	return &Config{
		StorePath:   StorePath,
		SecureNewDb: true,
		MetaStore:   StorePath,
		MetaFile:    loadMetaIdent().file,
	}
}

func writeMetaEntry(key string, value []byte) error {
	if metaStorage.rotatingKey.Load() {
		return errors.New(errDbRotating)
	}
	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		return err
	}
	defer pool.Release(metaPath)
	err = setDbEntry([]byte(key), value, db)
	return err
}

func getMetaEntry(key string) ([]byte, error) {
	if metaStorage.rotatingKey.Load() {
		return nil, errors.New(errDbRotating)
	}
	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		return nil, err
	}
	defer pool.Release(metaPath)

	value := make([]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(key))
		if e != nil {
			if errors.Is(e, badger.ErrKeyNotFound) {
				e = &EMetaKeyNotFound{
					Code:    8404,
					Message: "meta key not found",
					Wrapped: e,
				}
			}
			return e
		}
		// ValueCopy, not item.Value: the slice handed to a Value callback aliases
		// badger's internal buffers and is only valid for the life of the
		// transaction, but value escapes it. Passing the pre-allocated slice as
		// the destination keeps a zero-length result non-nil.
		value, e = item.ValueCopy(value)
		return e
	})
	return value, err
}

func writeMetaEvent(eventType EventType, comment string) error {
	now := time.Now().UnixMilli()
	event := Event{
		Type:    eventType,
		Comment: comment,
		TStamp:  now,
	}
	key := prefixMetaEvent + strconv.FormatInt(now, 10)
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return writeMetaEntry(key, value)
}

// WriteMetaConfig persists config to the meta database and records a
// configuration-change event.
func WriteMetaConfig(config *Config) error {
	value, err := json.Marshal(config)
	if err != nil {
		return err
	}
	err = writeMetaEntry(prefixMetaConfig, value)
	if err != nil {
		return err
	}
	err = writeMetaEvent(EventTypeConfigChange, "Updating config")
	return err
}

func getMetaConfig() (*Config, error) {
	entry, err := getMetaEntry(prefixMetaConfig)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = json.Unmarshal(entry, config)
	if err != nil {
		return nil, err
	}
	return config, err
}

func writeMetaDbObject(dbName string, dbObject *DbObject, isUpdate bool) error {
	jsonDb, err := json.Marshal(dbObject)
	if err != nil {
		return err
	}
	err = writeMetaEntry(prefixMetaDb+dbName, jsonDb)
	if err != nil {
		return err
	}
	if isUpdate {
		err = writeMetaEvent(EventTypeUpdate, "Updated db object: "+dbName)
	} else {
		err = writeMetaEvent(EventTypeCreate, "Created db object: "+dbName)
	}
	return err
}

func getMetaDbObject(dbName string) (*DbObject, error) {
	entry, err := getMetaEntry(prefixMetaDb + dbName)
	if err != nil {
		return nil, err
	}
	dbo := &DbObject{}
	if err = json.Unmarshal(entry, dbo); err != nil {
		return nil, err
	}
	// Reads must not mutate state: emitting a meta event here caused an encrypted
	// write on every data operation (GetEntry/InsertEntry/…) and made reads fail
	// outright during key rotation (writeMetaEntry returns errDbRotating). See H1.
	return dbo, nil
}

// WriteToKeyring stores value under key in the keyring database. It returns an
// error if a key rotation is currently in progress.
func WriteToKeyring(key string, value []byte) error {
	if keyStorage.rotatingKey.Load() {
		return errors.New(errDbRotating)
	}
	keyPath := path.Join(keyStorage.path, keyStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(keyPath, keyStorage.key)
	if err != nil {
		return err
	}
	defer pool.Release(keyPath)
	err = setDbEntry([]byte(key), value, db)
	return err
}

func getFromKeyring(key string) ([]byte, error) {
	if keyStorage.rotatingKey.Load() {
		return nil, errors.New(errDbRotating)
	}
	keyPath := path.Join(keyStorage.path, keyStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(keyPath, keyStorage.key)
	if err != nil {
		return nil, err
	}
	defer pool.Release(keyPath)

	value := make([]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(key))
		if e != nil {
			return e
		}
		// Copy out of the transaction — see getMetaEntry. This one matters most:
		// the escaping slice is an encryption key.
		value, e = item.ValueCopy(value)
		return e
	})
	return value, err
}

func randomValues(length int) ([]byte, error) {
	var alphaNum = []rune("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randoms := make([]rune, length)
	size := big.NewInt(int64(len(alphaNum)))
	for i := range randoms {
		index, _ := rand.Int(rand.Reader, size)
		randoms[i] = alphaNum[int(index.Int64())]
	}
	var dst = []byte(string(randoms))
	return dst, nil
}

func checkMetaFile() bool {
	id := loadMetaIdent()
	if id.path == "" || id.file == "" {
		return false
	}
	metaPath := path.Join(id.path, id.file)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return false
	}
	return true
}

func checkConfig() bool {
	if fxConfig == nil {
		return false
	}
	if fxConfig.StorePath != StorePath {
		return false
	}
	return true
}

func initKeyDb() error {
	keyStorage.path = StorePath
	keyStorage.file = lockDb
	err := genKeypair()
	if err != nil {
		return err
	}
	privatePath := path.Join(KeyPath, privateFile)
	hash, err := hashFile(privatePath)
	if err != nil {
		return err
	}
	keyStorage.key, err = deriveKeyFromHash(hash)
	if err != nil {
		return fmt.Errorf("error deriving key from hash: %v", err)
	}
	keyPath := path.Join(keyStorage.path, keyStorage.file)
	pool := GetConnectionPool()
	keyStorage.db, err = pool.Get(keyPath, keyStorage.key)
	if err != nil {
		return err
	}
	pool.Release(keyPath)
	return nil
}

func initMetaDb() error {
	fileKey, fErr := randomValues(fileKey)
	if fErr != nil {
		log.Println("Error generating random values:", fErr)
		return fErr
	}
	metaFile := "meta-" + string(fileKey)
	metaKey, fErr := randomValues(keyLength)
	if fErr != nil {
		log.Println("Error generating random values:", fErr)
		return fErr
	}
	storeMetaIdent(StorePath, metaFile, metaKey)
	metaPath := path.Join(StorePath, metaFile)
	pool := GetConnectionPool()
	var err error
	metaStorage.db, err = pool.Get(metaPath, metaKey)
	if err != nil {
		return err
	}
	fErr = WriteToKeyring(prefixMetaKey, metaKey)
	if fErr != nil {
		log.Println("Error saving key file to keyring:", fErr)
	}
	pool.Release(metaPath)
	_ = writeMetaEvent(EventTypeWrite, "wrote keyring")
	fxConfig = DefaultConfig()
	err = WriteMetaConfig(fxConfig)

	return err
}

func openKeyDb() error {
	keyStorage.path = StorePath
	keyStorage.file = lockDb
	keyPath := path.Join(keyStorage.path, keyStorage.file)
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		// I'm not finding the key db, init one
		err = initKeyDb()
		if err != nil {
			log.Println("Error opening/initialising key db: ", err)
			return err
		}
	}
	privatePath := path.Join(KeyPath, privateFile)
	if _, err := os.Stat(privatePath); os.IsNotExist(err) {
		return err
	}
	hash, err := hashFile(privatePath)
	if err != nil {
		return err
	}
	keyStorage.key, err = deriveKeyFromHash(hash)
	if err != nil {
		return err
	}
	pool := GetConnectionPool()
	keyStorage.db, err = pool.Get(keyPath, keyStorage.key)
	if err != nil {
		return err
	}
	pool.Release(keyPath)
	return nil
}

func openMetaDb() error {
	var latestMetaName string
	var latestMetaTimestamp int64 = 0
	entries, err := os.ReadDir(StorePath)
	if err != nil {
		log.Println("error reading store dir: ", err)
		return err
	}
	if len(entries) == 0 {
		err = initMetaDb()
		if err != nil {
			log.Println("Error opening/initialising meta db: ", err)
			return err
		}
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "meta-") {
			fInfo, e := entry.Info()
			if e != nil {
				log.Println("error reading file info: ", e)
				continue
			}
			fileTstamp := fInfo.ModTime().UnixMilli()
			if fileTstamp > latestMetaTimestamp {
				latestMetaTimestamp = fileTstamp
				latestMetaName = entry.Name()
			}
		}
	}
	if latestMetaTimestamp > 0 {
		key, e := getFromKeyring(prefixMetaKey)
		if e != nil {
			log.Println("error reading keyring for meta key: ", e)
			return e
		}
		storeMetaIdent(StorePath, latestMetaName, key)
		metaPath := path.Join(StorePath, latestMetaName)
		pool := GetConnectionPool()
		metaStorage.db, err = pool.Get(metaPath, key)
		if err != nil {
			return err
		}
		pool.Release(metaPath)
	} else {
		err = initMetaDb()
		if err != nil {
			log.Println("Error opening/initialising meta db: ", err)
			return err
		}
		return nil
	}
	fxConfig, err = getMetaConfig()
	return err
}

// GetStorageObject opens the named database and returns a *Storage handle for
// it, loading the encryption key from the keyring for secure databases. The
// returned handle holds a connection-pool reference; the caller must call
// Close on it when finished to release that reference.
func GetStorageObject(dbName string) (*Storage, error) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()

	// we need to know 3 things:
	// 1. does it have an entry in the meta storage?
	// 2. does it have actual db folder in store path?
	// 3. if it's secured, does it have key stored in keyring?
	dbObject, err := getMetaDbObject(dbName)
	if err != nil {
		log.Println("error getting db object: ", err)
		return nil, err
	}
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		log.Println("database file not found: ", err)
		return nil, err
	}
	var dbKey = make([]byte, 0)
	var b64Decoded = make([]byte, 0)
	if dbObject.Secure && dbObject.Active {
		dbKey, err = getFromKeyring(prefixMetaDb + dbName)
		if err != nil {
			log.Println("unable to find key for db: ", err)
			return nil, err
		}
		b64Decoded, err = b64Decode(string(dbKey))
		if err != nil {
			log.Println("unable to get db key: ", err)
			return nil, err
		}
	}
	db, err := GetConnectionPool().Get(dbPath, b64Decoded)
	if err != nil {
		log.Println("error opening database: ", err)
		return nil, err
	}
	storageObject := &Storage{
		db:   db,
		path: dbObject.DbPath,
		file: dbObject.DbFile,
		key:  dbKey,
		// dbPath is exactly the key used for pool.Get above; Close releases it.
		poolKey: dbPath,
	}
	return storageObject, nil
}

func openUnsecuredDb(path string) (*badger.DB, error) {
	return GetConnectionPool().Get(path, nil)
}

// OpenDatabase opens the BadgerDB at path with the given encryption key (pass
// nil for an unencrypted database) and returns the underlying handle. Most
// callers should go through the connection pool rather than opening directly.
func OpenDatabase(path string, key []byte) (*badger.DB, error) {
	opt := badger.DefaultOptions(path).WithEncryptionKey(key).WithEncryptionKeyRotationDuration(24 * time.Hour)
	opt.IndexCacheSize = 100 << 20
	db, err := badger.Open(opt)
	if err != nil {
		log.Println("Error opening database: ", err)
		return nil, err
	}
	return db, nil
}

// CloseDatabase closes the underlying BadgerDB handle directly. Prefer
// pool.Release(path) for pool-managed connections; this is retained for handles
// whose path is not readily available and for tests.
func CloseDatabase(db *badger.DB) error {
	// If the database is managed by the pool, we should really be using pool.Release(path).
	// However, CloseDatabase is used in some places where we have a *badger.DB but not its path easily available,
	// or in tests. For backward compatibility and safety, we still allow direct closing,
	// but the pool will handle its own lifecycle.
	return db.Close()
}

func setDbEntry(key []byte, value []byte, db *badger.DB) error {
	var err error
	err = db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Println("meta update error: ", err)
	}

	return err
}

func getDbEntry(key []byte, db *badger.DB) ([]byte, error) {
	var err error
	value := make([]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		// Copy out of the transaction — see getMetaEntry.
		value, err = item.ValueCopy(value)
		return err
	})
	if err != nil {
		log.Println("meta get error: ", err)
		return nil, err
	}
	return value, err
}

func listDatabases() (map[string]DbObject, error) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()

	if metaStorage.rotatingKey.Load() {
		return nil, errors.New(errDbRotating)
	}
	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		return nil, err
	}
	defer pool.Release(metaPath)
	m := make(map[string]DbObject)
	err = db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		prefix := []byte(prefixMetaDb)
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			item := iterator.Item()
			key := string(item.Key())
			var value DbObject
			valError := item.Value(func(val []byte) error {
				e := json.Unmarshal(val, &value)
				return e
			})
			if valError != nil {
				return valError
			}
			m[key] = value
		}
		return nil
	})
	return m, err
}

func metaBatchInsert(values *map[string][]byte) error {
	if metaStorage.rotatingKey.Load() {
		return errors.New(errDbRotating)
	}
	var err error
	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	// Use a local handle rather than mutating the shared metaStorage.db, which was
	// written here without holding globalStateMu (H2).
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		return err
	}
	defer pool.Release(metaPath)
	wb := db.NewWriteBatch()
	defer wb.Cancel()

	for key, val := range *values {
		err = wb.Set([]byte(key), val)
		if err != nil {
			log.Println("error writing value to batch: ", err)
		}
	}
	return wb.Flush()
}

func batchInsertGeneric(ctx context.Context, values *map[string][]byte, db *badger.DB) error {
	var err error
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for key, val := range *values {
		err = wb.Set([]byte(key), val)
		if err != nil {
			log.Println("error writing value to batch: ", err)
			return err
		}

		// Check for context cancellation during batch build
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return wb.Flush()
}

// batchDeleteGeneric removes every key in keys from db in a single write
// batch, the delete-side counterpart to batchInsertGeneric. Removing a key
// that isn't there is not an error, so callers don't have to check first.
func batchDeleteGeneric(ctx context.Context, keys []string, db *badger.DB) error {
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		if err := wb.Delete([]byte(key)); err != nil {
			log.Println("error adding key to delete batch: ", err)
			return err
		}

		// Check for context cancellation during batch build
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return wb.Flush()
}

func countRecords(prefix string, db *badger.DB, verbose bool) (int, error) {
	var err error
	count := 0
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if verbose {
				item := it.Item()
				k := item.Key()
				log.Println("key: ", string(k))
			}
			count += 1
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func copyMetas() (newPath string, newKey []byte, err error) {
	ctx := context.Background()
	startTime := time.Now()

	if metaStorage.rotatingKey.Load() {
		return "", nil, errors.New("rotate flag already raised")
	}
	srcIdent := loadMetaIdent()
	metaPath := path.Join(srcIdent.path, srcIdent.file)
	oldFile := srcIdent.file
	pool := GetConnectionPool()
	// Use a local handle rather than mutating the shared metaStorage.db (H2).
	srcDb, err := pool.Get(metaPath, srcIdent.key)
	if err != nil {
		metricsCollector.RecordOperation(ctx, "rotation", oldFile, time.Since(startTime), false)
		return "", nil, err
	}
	defer pool.Release(metaPath)

	metaStorage.rotatingKey.Store(true)
	// Always clear the flag on the way out, including the early error returns below.
	defer metaStorage.rotatingKey.Store(false)

	itemCount := 0
	err = srcDb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			itemCount++
		}
		return nil
	})
	if err != nil {
		log.Println("Error counting items in meta database: ", err)
	}

	newMetaKey, _ := randomValues(keyLength)
	metaFileRandom, _ := randomValues(10)
	newMetaFile := "meta-" + string(metaFileRandom)
	newMetaPath := path.Join(StorePath, newMetaFile)
	newDb, err := pool.Get(newMetaPath, newMetaKey)
	if err != nil {
		log.Println("Error opening new meta database: ", err)
		metricsCollector.RecordOperation(ctx, "rotation", oldFile, time.Since(startTime), false)
		return "", nil, err
	}
	defer pool.Release(newMetaPath)

	values := make(map[string][]byte)
	stream := srcDb.NewStream()
	stream.NumGo = 20
	stream.ChooseKey = func(item *badger.Item) bool {
		return bytes.HasPrefix(item.Key(), stream.Prefix)
	}
	stream.Send = func(buffer *z.Buffer) error {
		var list pb.KVList
		if serr := buffer.SliceIterate(func(slice []byte) error {
			kv := new(pb.KV)
			if uerr := proto.Unmarshal(slice, kv); uerr != nil {
				return uerr
			}
			list.Kv = append(list.Kv, kv)
			return nil
		}); serr != nil {
			return serr
		}
		for _, element := range list.Kv {
			values[string(element.Key)] = element.Value
		}
		return nil
	}
	// H3: check the stream error BEFORE writing. A failed read of the source data
	// must not proceed to write a partial/empty new meta DB and report success.
	if err = stream.Orchestrate(context.Background()); err != nil {
		log.Println("Error streaming source meta database: ", err)
		metricsCollector.RecordOperation(ctx, "rotation", oldFile, time.Since(startTime), false)
		return "", nil, err
	}
	if err = batchInsertGeneric(ctx, &values, newDb); err != nil {
		log.Println("Error writing rotated meta database: ", err)
		metricsCollector.RecordOperation(ctx, "rotation", oldFile, time.Since(startTime), false)
		return "", nil, err
	}

	// H3: commit the rotation. Persist the new meta key to the keyring first so a
	// restart's openMetaDb (which reads prefixMetaKey) can open the newest meta dir,
	// then atomically publish the new meta identity so readers pick it up lock-free.
	if err = WriteToKeyring(prefixMetaKey, newMetaKey); err != nil {
		log.Println("Error persisting rotated meta key to keyring: ", err)
		metricsCollector.RecordOperation(ctx, "rotation", oldFile, time.Since(startTime), false)
		return "", nil, err
	}
	storeMetaIdent(srcIdent.path, newMetaFile, newMetaKey)

	duration := time.Since(startTime)
	metricsCollector.RecordOperation(ctx, "rotation", newMetaFile, duration, true)
	metricsCollector.RecordKeyRotation(oldFile, newMetaFile, itemCount)

	log.Printf("Key rotation complete: %s -> %s (%d items in %v)\n",
		oldFile, newMetaFile, itemCount, duration)
	return newMetaFile, newMetaKey, nil
}

func b64Encode(input []byte) string {
	return base64.StdEncoding.EncodeToString(input)
}

func b64Decode(input string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(input)
}

func getDbKey(dbName string, dbObject *DbObject) ([]byte, error) {
	bDbKey := make([]byte, 0)
	var err error
	if dbObject.Secure {
		bDbKey, err = getFromKeyring(prefixMetaDb + dbName)
		if err != nil {
			return nil, err
		}
		dbKey, e := b64Decode(string(bDbKey))
		if e != nil {
			return nil, e
		}
		return dbKey, nil
	}
	return nil, nil
}

// CreateDatabase creates a new database named dbName. When secure is true a
// random encryption key is generated and stored in the keyring. It returns an
// error if a database with the same name already exists.
func CreateDatabase(dbName string, secure bool) error {
	ctx := context.Background()
	startTime := time.Now()

	// check first
	exist, err := databaseExist(dbName)
	if err != nil {
		metricsCollector.RecordOperation(ctx, "check", dbName, time.Since(startTime), false)
		return err
	}
	if exist {
		err = errors.New("database already exists")
		metricsCollector.RecordOperation(ctx, "check", dbName, time.Since(startTime), false)
		return err
	}

	globalStateMu.RLock()
	storePath := fxConfig.StorePath
	globalStateMu.RUnlock()

	// open db with name and optional key - store the key on keyring
	dbId, _ := randomValues(fileIdLength)
	dbActualName := dbName + "-" + string(dbId)
	dbPath := path.Join(storePath, dbActualName)
	pool := GetConnectionPool()
	if secure {
		key, secErr := randomValues(keyLength)
		if secErr != nil {
			metricsCollector.RecordEncryptionError("key_generation_failed")
			return secErr
		}
		_, secErr = pool.Get(dbPath, key)
		if secErr != nil {
			metricsCollector.RecordOperation(ctx, "open", dbName, time.Since(startTime), false)
			return secErr
		}
		b64Key := b64Encode(key)
		secErr = WriteToKeyring(prefixMetaDb+dbName, []byte(b64Key))
		if secErr != nil {
			metricsCollector.RecordOperation(ctx, "wrt-keyring", dbName, time.Since(startTime), false)
			return secErr
		}
	} else {
		_, err = pool.Get(dbPath, nil)
		if err != nil {
			metricsCollector.RecordOperation(ctx, "open", dbName, time.Since(startTime), false)
			return err
		}
	}
	defer pool.Release(dbPath)
	// create a new DbObject struct and store it in meta db
	dbObject := DbObject{
		DbPath:      fxConfig.StorePath,
		DbFile:      dbActualName,
		Secure:      secure,
		Created:     time.Now().UnixMilli(),
		Active:      true,
		LastRotated: 0,
		Deleted:     0,
	}
	err = writeMetaDbObject(dbName, &dbObject, false)
	if err != nil {
		metricsCollector.RecordOperation(ctx, "wrt-meta", dbName, time.Since(startTime), false)
		return err
	}
	duration := time.Since(startTime)
	success := err == nil

	metricsCollector.RecordOperation(ctx, "create", dbName, duration, success)
	if success {
		metricsCollector.RecordDatabaseCreated(dbName)
		log.Printf("Database %s created in %v\n", dbName, duration)
	} else {
		log.Printf("Failed to create database %s after %v: %v\n", dbName, duration, err)
	}
	return err
}

func databaseExist(dbName string) (bool, error) {
	// This is only an existence check; it doesn't keep the handle, so release the
	// pool reference immediately rather than pinning the connection forever.
	so, err := GetStorageObject(dbName)
	if err != nil {
		var metaKeyNotFound *EMetaKeyNotFound
		if errors.As(err, &metaKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	so.Close()
	return true, nil
}

// InsertEntry writes value under key in the named database, opening it (and
// loading its key, if secure) as needed. It returns an error if the system is
// shutting down or the database is inactive.
func InsertEntry(dbName string, key string, value []byte) error {
	ctx := context.Background()
	startTime := time.Now()

	// Check if we're shutting down
	if IsShuttingDown() {
		return errors.New("system is shutting down - operation rejected")
	}

	dbObject, err := getMetaDbObject(dbName)
	if err != nil {
		return err
	}
	if !dbObject.Active {
		return errors.New(dbName + " - " + errDbInactive)
	}
	dbKey, err := getDbKey(dbName, dbObject)
	if err != nil {
		return err
	}
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)
	pool := GetConnectionPool()
	var db *badger.DB
	if dbObject.Secure {
		db, err = pool.Get(dbPath, dbKey)
	} else {
		db, err = pool.Get(dbPath, nil)
	}
	if err != nil {
		return err
	}
	defer pool.Release(dbPath)

	err = setDbEntry([]byte(key), value, db)
	duration := time.Since(startTime)
	success := err == nil
	metricsCollector.RecordOperation(ctx, "write", dbName, duration, success)
	if !success {
		log.Printf("Write operation failed for %s:%s after %v\n", dbName, key, duration)
	}
	return err
}

// Close releases this handle's connection-pool reference. Callers that obtain a
// *Storage from GetStorageObject must call Close (typically via defer) when done,
// otherwise the connection stays pinned and the pool can never reclaim it. It is
// safe to call on a handle that is not pool-managed (poolKey == ""), and safe to
// call more than once.
func (t *Storage) Close() {
	if t.poolKey == "" {
		return
	}
	GetConnectionPool().Release(t.poolKey)
	t.poolKey = ""
}

// InsertEntry writes value under key in this database.
func (t *Storage) InsertEntry(key string, value []byte) error {
	return setDbEntry([]byte(key), value, t.db)
}

// UpdateEntry writes value under key in the named database. It is an alias for
// InsertEntry: writes are upserts.
func UpdateEntry(dbName string, key string, value []byte) error {
	return InsertEntry(dbName, key, value)
}

// UpdateEntry writes value under key in this database (an upsert).
func (t *Storage) UpdateEntry(key string, value []byte) error {
	return setDbEntry([]byte(key), value, t.db)
}

// RemoveEntry deletes key from the named database. It returns an error if the
// database is inactive.
func RemoveEntry(dbName string, key string) error {
	ctx := context.Background()
	startTime := time.Now()

	if IsShuttingDown() {
		return errors.New("system is shutting down - operation rejected")
	}

	dbObject, err := getMetaDbObject(dbName)
	if err != nil {
		return err
	}
	if !dbObject.Active {
		return errors.New(dbName + " - " + errDbInactive)
	}
	dbKey, err := getDbKey(dbName, dbObject)
	if err != nil {
		return err
	}
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)
	pool := GetConnectionPool()

	var db *badger.DB
	if dbObject.Secure {
		db, err = pool.Get(dbPath, dbKey)
	} else {
		db, err = pool.Get(dbPath, nil)
	}
	if err != nil {
		return err
	}
	defer pool.Release(dbPath)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	duration := time.Since(startTime)
	success := err == nil
	metricsCollector.RecordOperation(ctx, "remove_entry", dbName, duration, success)
	if err != nil {
		return err
	}
	_ = writeMetaEvent(EventTypeDelete, "Deleted entry: "+dbName+":"+key)
	return err
}

// RemoveEntry deletes key from this database and records a delete event.
func (t *Storage) RemoveEntry(key string) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	_ = writeMetaEvent(EventTypeDelete, "Deleted entry: "+t.file+":"+key)
	return err

}

// RemoveEntries deletes all of keys from this database in a single batch and
// records one delete event, rather than the transaction and event per key a
// RemoveEntry loop would cost. Removing a key that isn't there is not an
// error.
//
// A failed flush may have already applied some of the deletes, so the batch is
// not atomic. Deletes are idempotent, so a caller that retries the whole slice
// after an error still ends up in the right place.
func (t *Storage) RemoveEntries(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := batchDeleteGeneric(context.Background(), keys, t.db); err != nil {
		return err
	}
	_ = writeMetaEvent(EventTypeDelete, "Deleted batch data from db: "+t.file)
	return nil
}

// BatchInsert writes all entries to the named database in a single batch. It
// returns an error if the database is inactive.
func BatchInsert(dbName string, entries map[string][]byte) error {
	ctx := context.Background()
	dbObject, err := getMetaDbObject(dbName)
	if err != nil {
		return err
	}
	if !dbObject.Active {
		return errors.New(dbName + " - " + errDbInactive)
	}
	dbKey, err := getDbKey(dbName, dbObject)
	if err != nil {
		return err
	}
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)

	var db *badger.DB
	pool := GetConnectionPool()
	if dbObject.Secure {
		db, err = pool.Get(dbPath, dbKey)
	} else {
		db, err = pool.Get(dbPath, nil)
	}
	if err != nil {
		return err
	}
	defer pool.Release(dbPath)

	err = batchInsertGeneric(ctx, &entries, db)
	if err != nil {
		return err
	}
	return err
}

// BatchInsert writes all entries to this database in a single batch and records
// a write event.
func (t *Storage) BatchInsert(entries *map[string][]byte) error {
	ctx := context.Background()
	err := batchInsertGeneric(ctx, entries, t.db)
	_ = writeMetaEvent(EventTypeWrite, "Wrote batch data to db: "+t.file)
	return err
}

// GetEntry reads the value stored under key in the named database. It returns
// an error if the system is shutting down or the database is inactive.
func GetEntry(dbName string, key string) ([]byte, error) {
	ctx := context.Background()
	startTime := time.Now()

	if IsShuttingDown() {
		return nil, errors.New("system is shutting down - operation rejected")
	}

	dbObject, err := getMetaDbObject(dbName)
	if err != nil {
		return nil, err
	}
	if !dbObject.Active {
		return nil, errors.New(dbName + " - " + errDbInactive)
	}
	dbKey, err := getDbKey(dbName, dbObject)
	if err != nil {
		return nil, err
	}

	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)
	pool := GetConnectionPool()
	var db *badger.DB
	if dbObject.Secure {
		db, err = pool.Get(dbPath, dbKey)
	} else {
		db, err = pool.Get(dbPath, nil)
	}
	if err != nil {
		return nil, err
	}
	defer pool.Release(dbPath)

	value, err := getDbEntry([]byte(key), db)
	duration := time.Since(startTime)
	success := err == nil

	metricsCollector.RecordOperation(ctx, "read", dbName, duration, success)
	return value, err
}

// GetEntry reads the value stored under key in this database.
func (t *Storage) GetEntry(key string) ([]byte, error) {
	return getDbEntry([]byte(key), t.db)
}

// All returns every key-value pair in this database as a map, streamed
// concurrently from the underlying store.
func (t *Storage) All() (map[string][]byte, error) {
	m := make(map[string][]byte)
	db := t.db
	stream := db.NewStream()
	stream.NumGo = 20
	stream.LogPrefix = "stream -> "
	stream.KeyToList = nil
	stream.Send = func(buffer *z.Buffer) error {
		return buffer.SliceIterate(func(slice []byte) error {
			kv := new(pb.KV)
			if err := proto.Unmarshal(slice, kv); err != nil {
				return err
			}
			m[string(kv.Key)] = kv.Value
			return nil
		})
	}
	err := stream.Orchestrate(context.Background())
	return m, err
}

// ListDatabases returns the meta keys of all databases recorded in the meta
// database. It returns an error if a key rotation is in progress.
func ListDatabases() ([]string, error) {
	if metaStorage.rotatingKey.Load() {
		return nil, errors.New(errDbRotating)
	}
	metaPath, metaKey := metaPathAndKey()
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	if err != nil {
		return nil, err
	}
	defer pool.Release(metaPath)
	var dbList []string
	err = db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		prefix := []byte(prefixMetaDb)
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			item := iterator.Item()
			key := string(item.Key())
			err := item.Value(func(v []byte) error {
				dbList = append(dbList, key)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return dbList, err
}

// NewStorage wraps an already-open *badger.DB in a *Storage handle. The
// resulting handle is not pool-managed, so its Close is a no-op; the caller
// remains responsible for the underlying db's lifecycle.
func NewStorage(db *badger.DB, path string, file string, key []byte, rotating bool) *Storage {
	s := &Storage{
		db:   db,
		path: path,
		file: file,
		key:  key,
	}
	s.rotatingKey.Store(rotating)
	return s
}

// ListConfigurations returns the configuration persisted in the meta database.
func ListConfigurations() (*Config, error) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()

	if metaStorage.rotatingKey.Load() {
		return nil, errors.New(errDbRotating)
	}

	dbEntry, err := getMetaEntry(prefixMetaConfig)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = json.Unmarshal(dbEntry, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// UpdateConfigurations persists config to the meta database and, on success,
// makes it the active in-memory configuration.
func UpdateConfigurations(config *Config) error {
	globalStateMu.Lock()
	defer globalStateMu.Unlock()

	err := WriteMetaConfig(config)
	if err == nil {
		fxConfig = config
	}
	return err
}

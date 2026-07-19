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

// meta db stores the list of databases we have, etc.
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

func DefaultConfig() *Config {
	return &Config{
		StorePath:   StorePath,
		SecureNewDb: true,
		MetaStore:   StorePath,
		MetaFile:    metaStorage.file,
	}
}

func writeMetaEntry(key string, value []byte) error {
	if metaStorage.rotatingKey {
		return errors.New(errDbRotating)
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaStorage.key)
	if err != nil {
		return err
	}
	defer pool.Release(metaPath)
	err = setDbEntry([]byte(key), value, db)
	return err
}

func getMetaEntry(key string) ([]byte, error) {
	if metaStorage.rotatingKey {
		return nil, errors.New(errDbRotating)
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaStorage.key)
	if err != nil {
		return nil, err
	}
	defer pool.Release(metaPath)

	value := make([]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(key))
		if e != nil {
			if errors.Is(e, badger.ErrKeyNotFound) ||
				(e.Error() != "" && strings.Contains(e.Error(), "key not found")) {
				e = &EMetaKeyNotFound{
					Code:    8404,
					Message: "meta key not found",
					Wrapped: e,
				}
			}
			return e
		}
		e = item.Value(func(val []byte) error {
			value = val
			return nil
		})
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
	err = json.Unmarshal(entry, dbo)
	if err != nil {
		return nil, err
	}
	err = writeMetaEvent(EventTypeRead, "Read meta db object: "+prefixMetaDb+dbName)
	return dbo, err
}

func WriteToKeyring(key string, value []byte) error {
	if keyStorage.rotatingKey {
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
	if keyStorage.rotatingKey {
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
		e = item.Value(func(val []byte) error {
			value = val
			return nil
		})
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
	if metaStorage.path == "" || metaStorage.file == "" {
		return false
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
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
	metaStorage.path = StorePath
	metaStorage.file = "meta-" + string(fileKey)
	metaStorage.key, fErr = randomValues(keyLength)
	if fErr != nil {
		log.Println("Error generating random values:", fErr)
		return fErr
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	var err error
	metaStorage.db, err = pool.Get(metaPath, metaStorage.key)
	if err != nil {
		return err
	}
	fErr = WriteToKeyring(prefixMetaKey, metaStorage.key)
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
	extractedKey, err := extractString(hash, keyLength)
	if err != nil {
		return err
	}
	keyStorage.key = []byte(extractedKey)
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
		metaStorage.path = StorePath
		metaStorage.file = latestMetaName
		key, e := getFromKeyring(prefixMetaKey)
		if e != nil {
			log.Println("error reading keyring for meta key: ", err)
			return err
		}
		metaStorage.key = key
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
		if db == nil {
			return nil, err
		}
	}
	storageObject := &Storage{
		db:          db,
		path:        dbObject.DbPath,
		file:        dbObject.DbFile,
		key:         dbKey,
		rotatingKey: false,
	}
	return storageObject, nil
}

func openUnsecuredDb(path string) (*badger.DB, error) {
	return GetConnectionPool().Get(path, nil)
}

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
		e := item.Value(func(val []byte) error {
			value = val
			return nil
		})
		return e
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

	if metaStorage.rotatingKey {
		return nil, errors.New(errDbRotating)
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaStorage.key)
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
	if metaStorage.rotatingKey {
		return errors.New(errDbRotating)
	}
	var err error
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	metaStorage.db, err = pool.Get(metaPath, metaStorage.key)
	if err != nil {
		return err
	}
	defer pool.Release(metaPath)
	wb := metaStorage.db.NewWriteBatch()
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

	if metaStorage.rotatingKey {
		return "", nil, errors.New("rotate flag already raised")
	}
	var e error
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	metaStorage.db, e = pool.Get(metaPath, metaStorage.key)
	if e != nil {
		metricsCollector.RecordOperation(ctx, "rotation", metaStorage.file, time.Since(startTime), false)
		return "", nil, e
	}
	defer pool.Release(metaPath)

	metaStorage.rotatingKey = true

	itemCount := 0
	err = metaStorage.db.View(func(txn *badger.Txn) error {
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
		metricsCollector.RecordOperation(ctx, "rotation", metaStorage.file, time.Since(startTime), false)
		return "", nil, err
	}
	defer pool.Release(newMetaPath)

	values := make(map[string][]byte)
	stream := metaStorage.db.NewStream()
	stream.NumGo = 20
	stream.ChooseKey = func(item *badger.Item) bool {
		return bytes.HasPrefix(item.Key(), stream.Prefix)
	}
	stream.Send = func(buffer *z.Buffer) error {
		var list pb.KVList
		err = buffer.SliceIterate(func(slice []byte) error {
			kv := new(pb.KV)
			if err = proto.Unmarshal(slice, kv); err != nil {
				return err
			}
			list.Kv = append(list.Kv, kv)
			return nil
		})
		if err != nil {
			return err
		}
		for _, element := range list.Kv {
			key := element.Key
			value := element.Value
			values[string(key)] = value
		}
		return err
	}
	err = stream.Orchestrate(context.Background())
	err = batchInsertGeneric(ctx, &values, newDb)
	metaStorage.rotatingKey = false
	// We MUST close it to release the lock because rotation might be followed by other operations
	// that expect the lock to be available, or tests that check the file.
	// But since we are using the pool, we just release it.
	pool.Release(newMetaPath)

	duration := time.Since(startTime)
	metricsCollector.RecordOperation(ctx, "rotation", metaStorage.file, duration, true)
	metricsCollector.RecordKeyRotation(metaStorage.file, newMetaFile, itemCount)

	log.Printf("Key rotation complete: %s -> %s (%d items in %v)\n",
		metaStorage.file, newMetaFile, itemCount, duration)
	return newMetaFile, newMetaKey, err
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
	_, err := GetStorageObject(dbName)
	if err != nil {
		var metaKeyNotFound *EMetaKeyNotFound
		if errors.As(err, &metaKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

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

func (t *Storage) InsertEntry(key string, value []byte) error {
	return setDbEntry([]byte(key), value, t.db)
}

func UpdateEntry(dbName string, key string, value []byte) error {
	return InsertEntry(dbName, key, value)
}

func (t *Storage) UpdateEntry(key string, value []byte) error {
	return setDbEntry([]byte(key), value, t.db)
}

func RemoveEntry(dbName string, key string) error {
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
	if err != nil {
		return err
	}
	_ = writeMetaEvent(EventTypeDelete, "Deleted entry: "+dbName+":"+key)
	return err
}

func (t *Storage) RemoveEntry(key string) error {
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	_ = writeMetaEvent(EventTypeDelete, "Deleted entry: "+t.file+":"+key)
	return err

}

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

func (t *Storage) BatchInsert(entries *map[string][]byte) error {
	ctx := context.Background()
	err := batchInsertGeneric(ctx, entries, t.db)
	_ = writeMetaEvent(EventTypeWrite, "Wrote batch data to db: "+t.file)
	return err
}

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

func (t *Storage) GetEntry(key string) ([]byte, error) {
	return getDbEntry([]byte(key), t.db)
}

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

func ListDatabases() ([]string, error) {
	if metaStorage.rotatingKey {
		return nil, errors.New(errDbRotating)
	}
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaStorage.key)
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

func NewStorage(db *badger.DB, path string, file string, key []byte, rotating bool) *Storage {
	return &Storage{
		db:          db,
		path:        path,
		file:        file,
		key:         key,
		rotatingKey: rotating,
	}
}

func ListConfigurations() (*Config, error) {
	globalStateMu.RLock()
	defer globalStateMu.RUnlock()

	if metaStorage.rotatingKey {
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

func UpdateConfigurations(config *Config) error {
	globalStateMu.Lock()
	defer globalStateMu.Unlock()

	err := WriteMetaConfig(config)
	if err == nil {
		fxConfig = config
	}
	return err
}

func Cache(value []byte, duration time.Duration) error {
	return nil
}

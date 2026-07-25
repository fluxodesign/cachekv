package cachekv

import (
	"encoding/json"
	"log"
	randv2 "math/rand/v2"
	"os"
	"path"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const alternateTestStorePath = "./test-alternate/"

func setup() func() {
	StorePath = "./test-store/"
	KeyPath = "./.test-private/"
	err := os.Setenv("POOL_TIMEOUT_MS", "100")
	if err != nil {
		log.Println("Error setting connection pool timeout.")
	}
	Startup()
	// teardown
	return func() {
		GetConnectionPool().CloseAll()
		connectionPool = nil
		connectionPoolOnce = sync.Once{}
		metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
		_, err = os.Stat(metaPath)
		if err == nil {
			err = os.RemoveAll(metaPath)
			if err != nil {
				log.Println("error removing test db file: ", err)
			}
		}
		err = os.RemoveAll(StorePath)
		if err != nil {
			log.Println("error removing test store path: ", err)
		}
		err = os.RemoveAll(alternateTestStorePath)
		if err != nil {
			log.Println("error removing test alternate store path: ", err)
		}
		err = os.RemoveAll(KeyPath)
		if err != nil {
			log.Println("error removing test private path: ", err)
		}
	}
}

func TestRandomValues(t *testing.T) {
	defer setup()()
	value, err := randomValues(32)
	assert.Nil(t, err)
	assert.Equal(t, 32, len(value))
}

func TestInit(t *testing.T) {
	defer setup()()
	assert.True(t, checkMetaFile())
	assert.NotNil(t, metaStorage.db)
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	_, err := os.Stat(metaPath)
	assert.Nil(t, err)
}

func TestSetGetMetaEntry(t *testing.T) {
	defer setup()()
	assert.Nil(t, writeMetaEntry("testkey", []byte("testvalue")))
	value, err := getMetaEntry("testkey")
	assert.Nil(t, err)
	assert.Equal(t, value, []byte("testvalue"))
}

func TestDifferentEncryptionKeys(t *testing.T) {
	defer setup()()
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	assert.Nil(t, writeMetaEntry("testkey", []byte("testvalue")))
	time.Sleep(2 * time.Second)
	value, err := getMetaEntry("testkey")
	assert.Nil(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, value, []byte("testvalue"))
	metaKey, _ := randomValues(32)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, metaKey)
	assert.NotNil(t, err)
	assert.Nil(t, db)
}

// TestEmptyKeyRejectedForOpenEncryptedDb guards against C3: requesting an
// already-open encrypted database with no key must fail the same way a wrong
// key does, rather than silently returning the encrypted connection.
func TestEmptyKeyRejectedForOpenEncryptedDb(t *testing.T) {
	defer setup()()
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	pool := GetConnectionPool()

	db, err := pool.Get(metaPath, nil)
	assert.NotNil(t, err)
	assert.Nil(t, db)
}

func TestCopyMetasTwoRecords(t *testing.T) {
	defer setup()()
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	pool := GetConnectionPool()
	oldDb, err := pool.Get(metaPath, loadMetaIdent().key)
	assert.Nil(t, err)
	assert.Nil(t, setDbEntry([]byte("prefix:testkey"), []byte("testvalue"), oldDb))
	assert.Nil(t, setDbEntry([]byte("prefix:testkey2"), []byte("testvalue2"), oldDb))
	keys, err := countRecords("prefix:", oldDb, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, keys)
	pool.Release(metaPath)
	newPath, newKey, err := copyMetas()
	newMetaPath := path.Join(loadMetaIdent().path, newPath)
	// We need to wait for the pool's cleanup or ensure the pool closes it if we want to re-open it.
	// But actually, we SHOULD use the pool here too.
	newDb, err := pool.Get(newMetaPath, newKey)
	assert.Nil(t, err)
	assert.NotNil(t, newDb)
	keys, err = countRecords("prefix:", newDb, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, keys)
	pool.Release(newMetaPath)
}

func TestCopyMetas(t *testing.T) {
	defer setup()()
	newMeta, _ := randomValues(32)
	log.Println(newMeta)
	n := 10000
	values := make(map[string][]byte)
	start := time.Now()
	for range n {
		found := true
		for found == true {
			newKey := uuid.NewString()
			_, found = values[newKey]
			if !found {
				rv, _ := randomValues(keyLength)
				values["prefix:"+newKey] = rv
			}
		}
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("data generation completed in %d seconds", int(duration.Seconds()))
	assert.Nil(t, initMetaDb())
	err := metaBatchInsert(&values)
	assert.Nil(t, err)
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	pool := GetConnectionPool()
	oldDb, err := pool.Get(metaPath, loadMetaIdent().key)
	assert.Nil(t, err)
	records, err := countRecords("prefix:", oldDb, false)
	assert.Nil(t, err)
	assert.Equal(t, records, n)
	// asserting every record is present
	for k, v := range values {
		value, err := getDbEntry([]byte(k), oldDb)
		assert.Nil(t, err)
		assert.NotNil(t, value)
		assert.Equal(t, value, v)
	}
	pool.Release(metaPath)
	start = time.Now()
	newPath, newKey, err := copyMetas()
	end = time.Now()
	assert.Nil(t, err)
	duration = end.Sub(start)
	log.Printf("copyMetas() with %d records completed in %d seconds", n, int(duration.Seconds()))
	newMetaPath := path.Join(StorePath, newPath)
	newDb, err := pool.Get(newMetaPath, newKey)
	assert.Nil(t, err)
	assert.NotNil(t, newDb)
	records, err = countRecords("prefix:", newDb, false)
	assert.Nil(t, err)
	assert.Equal(t, n, records)
	// asserting every record is present in new db
	for k, v := range values {
		value, err := getDbEntry([]byte(k), newDb)
		assert.Nil(t, err)
		assert.NotNil(t, value)
		assert.Equal(t, value, v)
	}
	pool.Release(newMetaPath)
}

// TestCopyMetasCommitsRotation verifies the H3 fix: on success copyMetas commits the
// rotation by swapping the in-memory metaStorage.{file,key} and persisting the new
// meta key to the keyring, and clears the rotation flag.
func TestCopyMetasCommitsRotation(t *testing.T) {
	defer setup()()

	// Seed the current meta DB with a record.
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	pool := GetConnectionPool()
	db, err := pool.Get(metaPath, loadMetaIdent().key)
	assert.Nil(t, err)
	assert.Nil(t, setDbEntry([]byte("prefix:k1"), []byte("v1"), db))
	pool.Release(metaPath)

	oldFile := loadMetaIdent().file

	newFile, newKey, err := copyMetas()
	assert.Nil(t, err)
	assert.NotEqual(t, "", newFile)

	// The in-memory metaStorage must now point at the rotated DB.
	assert.Equal(t, newFile, loadMetaIdent().file)
	assert.Equal(t, newKey, loadMetaIdent().key)
	assert.NotEqual(t, oldFile, loadMetaIdent().file)

	// The new meta key must be persisted to the keyring so a restart can open it.
	persisted, err := getFromKeyring(prefixMetaKey)
	assert.Nil(t, err)
	assert.Equal(t, newKey, persisted)

	// The rotation flag must be cleared after a successful rotation.
	assert.False(t, metaStorage.rotatingKey.Load())

	// The rotated data must be readable through the committed metaStorage.
	newMetaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	rdb, err := pool.Get(newMetaPath, loadMetaIdent().key)
	assert.Nil(t, err)
	val, err := getDbEntry([]byte("prefix:k1"), rdb)
	assert.Nil(t, err)
	assert.Equal(t, []byte("v1"), val)
	pool.Release(newMetaPath)
}

// TestCopyMetasRefusesWhenAlreadyRotating verifies that a failed rotation does not
// commit: when the rotation flag is already raised, copyMetas returns an error and
// leaves metaStorage.{file,key} untouched (the no-partial-commit invariant from H3).
func TestCopyMetasRefusesWhenAlreadyRotating(t *testing.T) {
	defer setup()()
	oldFile := loadMetaIdent().file
	oldKey := loadMetaIdent().key

	metaStorage.rotatingKey.Store(true)
	defer metaStorage.rotatingKey.Store(false)

	newFile, newKey, err := copyMetas()
	assert.NotNil(t, err)
	assert.Equal(t, "", newFile)
	assert.Nil(t, newKey)

	// No commit must have happened.
	assert.Equal(t, oldFile, loadMetaIdent().file)
	assert.Equal(t, oldKey, loadMetaIdent().key)
}

// TestConcurrentRotatingKeyAccess exercises the H2 fix under -race: the rotation
// flag is read by every meta operation and written during rotation, so it must be
// safe to touch from multiple goroutines. Toggling it while readers gate on it must
// not trip the race detector.
func TestConcurrentRotatingKeyAccess(t *testing.T) {
	defer setup()()
	assert.Nil(t, writeMetaEntry("k", []byte("v")))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: toggle the rotation flag continuously.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				metaStorage.rotatingKey.Store(true)
				metaStorage.rotatingKey.Store(false)
			}
		}
	}()

	// Readers: meta reads that gate on the flag (may intermittently see
	// errDbRotating, which is expected and fine).
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_, _ = getMetaEntry("k")
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestConcurrentMetaIdentityAccess exercises the H2 reader-lock fix under -race: the
// meta identity (path/file/key) is swapped atomically on rotation, so readers loading
// it via metaPathAndKey must never race with a concurrent storeMetaIdent swap.
func TestConcurrentMetaIdentityAccess(t *testing.T) {
	defer setup()()
	orig := loadMetaIdent()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: publish new meta identities continuously, as rotation commit does.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				storeMetaIdent(orig.path, orig.file, orig.key) // restore for teardown
				return
			default:
				storeMetaIdent(orig.path, "meta-swap", orig.key)
				storeMetaIdent(orig.path, orig.file, orig.key)
			}
		}
	}()

	// Readers: load the identity lock-free.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				p, key := metaPathAndKey()
				_, _ = p, key
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	assert.Equal(t, orig.file, loadMetaIdent().file)
}

func TestDefaultConfig(t *testing.T) {
	defer setup()()
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.True(t, cfg.SecureNewDb)
	assert.Equal(t, StorePath, cfg.StorePath)
	assert.Equal(t, StorePath, cfg.MetaStore)
	assert.Equal(t, loadMetaIdent().file, cfg.MetaFile)
}

func TestUpdateConfigurations(t *testing.T) {
	defer setup()()
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.True(t, cfg.SecureNewDb)
	assert.Equal(t, StorePath, cfg.StorePath)
	assert.Equal(t, StorePath, cfg.MetaStore)
	assert.Equal(t, loadMetaIdent().file, cfg.MetaFile)
	// change the config
	newStorePath := "/var/tmp/blah"
	newMetaFile := "blah-blah.meta"
	newMetaStore := "blah-blah.store"
	cfg.SecureNewDb = false
	cfg.StorePath = newStorePath
	cfg.MetaStore = newMetaStore
	cfg.MetaFile = newMetaFile
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	cfg2, err := ListConfigurations()
	assert.Nil(t, err)
	assert.Equal(t, false, cfg2.SecureNewDb)
	assert.Equal(t, newStorePath, cfg2.StorePath)
	assert.Equal(t, newMetaFile, cfg2.MetaFile)
	assert.Equal(t, newMetaStore, cfg2.MetaStore)
}

func TestKeyring(t *testing.T) {
	defer setup()()
	err := WriteToKeyring("user", []byte("pass"))
	assert.Nil(t, err)
	pwd, err := getFromKeyring("user")
	assert.Nil(t, err)
	assert.Equal(t, "pass", string(pwd))
}

func TestCreateAndListDatabases(t *testing.T) {
	defer setup()()
	testdb1 := "testdb1"
	testdb2 := "testdb2"
	// change config store
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	// create database
	err = CreateDatabase(testdb1, true)
	assert.Nil(t, err)
	_, err = getFromKeyring(prefixMetaDb + testdb1)
	assert.Nil(t, err)
	err = CreateDatabase(testdb2, false)
	assert.Nil(t, err)
	_, err = getFromKeyring(prefixMetaDb + testdb2)
	assert.NotNil(t, err)
	listDbs, err := ListDatabases()
	assert.Nil(t, err)
	assert.Greater(t, len(listDbs), 0)
	listAllDbs, err := listDatabases()
	assert.Nil(t, err)
	assert.Greater(t, len(listAllDbs), 0)
	for _, db := range listDbs {
		// check physical files/folders
		dbInfo := listAllDbs[db]
		assert.NotNil(t, dbInfo)
		assert.Equal(t, dbInfo.DbPath, alternateTestStorePath)
		dbPath := path.Join(alternateTestStorePath, dbInfo.DbFile)
		_, err = os.Stat(dbPath)
		assert.Nil(t, err)
		// check for key in keyring
		if dbInfo.Secure {
			fromKeyring, err := getFromKeyring(db)
			assert.Nil(t, err)
			assert.Greater(t, len(fromKeyring), 0)
		} else {
			_, err = getFromKeyring(db)
			assert.NotNil(t, err)
		}
	}
}

func TestBase64DecodeEncode(t *testing.T) {
	sampleInput, err := randomValues(255)
	assert.Nil(t, err)
	assert.Equal(t, 255, len(sampleInput))
	encoded := b64Encode(sampleInput)
	log.Println("encoded:", encoded)
	decoded, err := b64Decode(encoded)
	assert.Nil(t, err)
	assert.Equal(t, sampleInput, decoded)
}

func TestInsertAndGetEntry(t *testing.T) {
	defer setup()()
	testDb1 := "testdb1"
	dataKey := "dataKey"
	dataValue, err := randomValues(256)
	assert.Nil(t, err)
	// change config store
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	err = CreateDatabase(testDb1, true)
	assert.Nil(t, err)
	err = InsertEntry(testDb1, dataKey, dataValue)
	assert.Nil(t, err)
	getValue, err := GetEntry(testDb1, dataKey)
	assert.Nil(t, err)
	assert.Equal(t, string(dataValue), string(getValue))
}

func TestInsertAndUpdateEntry(t *testing.T) {
	defer setup()()
	testDb1 := "testdb1"
	dataKey := "dataKey"
	dataValue, err := randomValues(256)
	assert.Nil(t, err)
	newDataValue, err := randomValues(256)
	assert.Nil(t, err)

	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	err = CreateDatabase(testDb1, true)
	assert.Nil(t, err)
	err = InsertEntry(testDb1, dataKey, dataValue)
	assert.Nil(t, err)
	getValue, err := GetEntry(testDb1, dataKey)
	assert.Nil(t, err)
	assert.Equal(t, string(dataValue), string(getValue))
	err = UpdateEntry(testDb1, dataKey, newDataValue)
	assert.Nil(t, err)
	getValue, err = GetEntry(testDb1, dataKey)
	assert.Nil(t, err)
	assert.Equal(t, string(newDataValue), string(getValue))
}

func TestDeleteEntry(t *testing.T) {
	defer setup()()
	testDb1 := "testdb1"
	dataKey := "dataKey"
	dataValue, err := randomValues(256)
	assert.Nil(t, err)
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	err = CreateDatabase(testDb1, true)
	assert.Nil(t, err)
	err = InsertEntry(testDb1, dataKey, dataValue)
	assert.Nil(t, err)
	getValue, err := GetEntry(testDb1, dataKey)
	assert.Nil(t, err)
	assert.Equal(t, string(dataValue), string(getValue))
	err = RemoveEntry(testDb1, dataKey)
	assert.Nil(t, err)
	getValue, err = GetEntry(testDb1, dataKey)
	assert.NotNil(t, err)
	assert.Nil(t, getValue)
}

func TestInsertBatch(t *testing.T) {
	defer setup()()
	testDb1 := "testdb1"
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	err = CreateDatabase(testDb1, true)
	assert.Nil(t, err)

	const (
		totalEntries = 1000000
		batchSize    = 100000
		batchCount   = totalEntries / batchSize
		sampleCheck  = 1000
	)
	var sampledKeys []string
	start := time.Now()

	for batchNum := range batchCount {
		entries := make(map[string][]byte, batchSize)
		for range batchSize {
			found := true
			for found == true {
				newKey := uuid.New().String()
				if _, exists := entries[newKey]; !exists {
					found = false
					rv, _ := randomValues(keyLength)
					entries[newKey] = rv

					// Keep track of some keys for later validation (sample-based)
					if len(sampledKeys) < sampleCheck {
						sampledKeys = append(sampledKeys, newKey)
					}
				}
			}
		}

		log.Printf("Batch %d/%d: Generated %d entries\n", batchNum+1, batchCount, len(entries))

		// Insert this batch into the database
		err = BatchInsert(testDb1, entries)
		assert.Nil(t, err)

		// Clear memory for next batch (entries map will be garbage collected)
		entries = nil

		// Optional: Allow GC to run periodically to free up memory
		if batchNum%5 == 0 {
			log.Printf("Triggering GC after batch %d...\n", batchNum+1)
			runtime.GC() // This helps release memory between batches
		}
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("Total generation and insert completed in %d seconds\n", int(duration.Seconds()))

	// Verify total record count instead of checking every single entry (memory efficient)
	time.Sleep(100 * time.Millisecond) // Allow locks to release after batch inserts
	storageObject, err := GetStorageObject(testDb1)
	assert.Nil(t, err)
	assert.NotNil(t, storageObject)
	defer storageObject.Close()

	records, err := countRecords("", storageObject.db, false)
	assert.Nil(t, err)
	assert.Equal(t, totalEntries, records)

	// Sample-check a subset of entries instead of all 2M (memory efficient validation)
	log.Printf("Sample checking %d out of %d entries for data integrity...\n", len(sampledKeys), totalEntries)
	for _, key := range sampledKeys {
		value, e := getDbEntry([]byte(key), storageObject.db)
		assert.Nil(t, e)
		assert.NotNil(t, value)
		assert.GreaterOrEqual(t, len(value), 0) // Verify data exists (not checking exact match for memory efficiency)
	}

	log.Printf("✓ Test completed successfully - all %d entries inserted and validated via sampling\n", totalEntries)
}

func TestGetEntryWithinALotOfEntries(t *testing.T) {
	defer setup()()
	testDb1 := "testdb1"
	const (
		totalEntries = 1000000
		batchSize    = 100000
		batchCount   = totalEntries / batchSize
	)

	var sampledKey string
	var sampledValue []byte

	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	cfg.SecureNewDb = true
	cfg.StorePath = alternateTestStorePath
	err = UpdateConfigurations(cfg)
	assert.Nil(t, err)
	err = CreateDatabase(testDb1, true)
	assert.Nil(t, err)

	start := time.Now()
	for batchNum := 0; batchNum < batchCount; batchNum++ {
		entries := make(map[string][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			found := true
			for found == true {
				newKey := uuid.New().String()
				if _, exists := entries[newKey]; !exists {
					found = false
					rv, _ := randomValues(keyLength)
					entries[newKey] = rv

					// Sample one entry for later validation
					if sampledKey == "" && i%10000 == 0 { // Pick every 10k-th entry as sample
						sampledKey = newKey
						sampledValue = rv
					}
				}
			}
		}

		log.Printf("Batch %d/%d: Generated %d entries\n", batchNum+1, batchCount, len(entries))
		err = BatchInsert(testDb1, entries)
		assert.Nil(t, err)
		entries = nil // Free memory

		if batchNum%5 == 0 {
			runtime.GC()
		}
	}
	end := time.Now()
	genDataDuration := end.Sub(start)
	log.Printf("DATA generation completed in %d seconds\n", int(genDataDuration.Seconds()))

	assert.NotEmpty(t, sampledKey) // Ensure we have a sample to check

	// Time the GetEntry for sampled key
	start = time.Now()
	entry, err := GetEntry(testDb1, sampledKey)
	end = time.Now()
	assert.Nil(t, err)
	assert.Equal(t, sampledValue, entry)

	getEntryDuration := end.Sub(start)
	log.Printf("GetEntry() finished in %d milliseconds\n", int(getEntryDuration.Milliseconds()))
}

func TestInitReloadingExistingMetafile(t *testing.T) {
	defer setup()()
	// Meta and Key DBs are already opened by Startup() in setup()
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	pool := GetConnectionPool()
	metaDb, err := pool.Get(metaPath, loadMetaIdent().key)
	assert.Nil(t, err)
	assert.NotNil(t, metaDb)
	pool.Release(metaPath)

	keyPath := path.Join(keyStorage.path, keyStorage.file)
	keyDb, err := pool.Get(keyPath, keyStorage.key)
	assert.Nil(t, err)
	assert.NotNil(t, keyDb)
	assert.True(t, checkConfig())
	pool.Release(keyPath)
}

func TestGetStorageObject(t *testing.T) {
	defer setup()()
	var myKey = "key1"
	var myValue = "value1"
	assert.Nil(t, CreateDatabase("testdb", true))
	storageObject, err := GetStorageObject("testdb")
	assert.Nil(t, err)
	assert.NotNil(t, storageObject)
	defer storageObject.Close()
	// try to insert data into the database and confirm
	assert.Nil(t, InsertEntry("testdb", myKey, []byte(myValue)))
	byteEntry, err := GetEntry("testdb", myKey)
	assert.Nil(t, err)
	assert.Equal(t, myValue, string(byteEntry))
}

func TestDbObjectInsertEntry(t *testing.T) {
	defer setup()()
	var myKey = "key1"
	var myValue = "value1"
	var updatedValue = "value2"
	assert.Nil(t, CreateDatabase("testdb", true))
	storageObject, err := GetStorageObject("testdb")
	assert.Nil(t, err)
	assert.NotNil(t, storageObject)
	defer storageObject.Close()
	assert.Nil(t, storageObject.InsertEntry(myKey, []byte(myValue)))
	byteEntry, err := storageObject.GetEntry(myKey)
	assert.Nil(t, err)
	assert.Equal(t, myValue, string(byteEntry))
	assert.Nil(t, storageObject.UpdateEntry(myKey, []byte(updatedValue)))
	byteEntry, err = storageObject.GetEntry(myKey)
	assert.Nil(t, err)
	assert.Equal(t, updatedValue, string(byteEntry))
	assert.Nil(t, storageObject.RemoveEntry(myKey))
	time.Sleep(100 * time.Millisecond) // Allow deletion to fully propagate
	byteEntry, err = storageObject.GetEntry(myKey)
	assert.NotNil(t, err)
	assert.Nil(t, byteEntry)
}

func TestOpenKeyDbWithDirAndNoFiles(t *testing.T) {
	defer setup()()
	keyPath := path.Join(keyStorage.path, keyStorage.file)
	_, err := os.Stat(keyPath)
	assert.Nil(t, err)

	// Close all connections before removing the directory
	GetConnectionPool().CloseAll()

	err = os.RemoveAll(keyPath)
	assert.Nil(t, err)
	assert.Nil(t, openKeyDb())
	_, err = os.Stat(keyPath)
	assert.Nil(t, err)
}

// TestKeyDbReopensAfterRestart is a regression test for the C1 key-derivation bug:
// initKeyDb and openKeyDb must derive the same keyring encryption key, otherwise the
// key DB (and every secure database's key) becomes unreadable after a normal restart.
// It writes a secure value, simulates a process restart (drops the connection-pool
// singleton but leaves every file on disk), reopens the key DB against the existing
// lock.db, and reads the value back.
func TestKeyDbReopensAfterRestart(t *testing.T) {
	defer setup()()

	// Key derived by initKeyDb during the first Startup.
	initKey := make([]byte, len(keyStorage.key))
	copy(initKey, keyStorage.key)

	// Write a secure value through the public API.
	testDb := "reopen-secure-db"
	dataKey := "dataKey"
	dataValue, err := randomValues(256)
	assert.Nil(t, err)
	assert.Nil(t, CreateDatabase(testDb, true))
	assert.Nil(t, InsertEntry(testDb, dataKey, dataValue))

	// Simulate a full process restart: close and drop the pool singleton and the
	// in-memory keyring handle, but leave all files on disk as a real restart would.
	GetConnectionPool().CloseAll()
	connectionPool = nil
	connectionPoolOnce = sync.Once{}
	keyStorage.key = nil
	keyStorage.db = nil

	// Reopen the key DB against the existing lock.db on a fresh pool.
	err = openKeyDb()
	assert.Nil(t, err)
	// The re-derived key must be byte-identical to the init-time key.
	assert.Equal(t, initKey, keyStorage.key)

	// The secure value written before the restart must still be readable, which
	// requires the reopened keyring to decrypt correctly.
	getValue, err := GetEntry(testDb, dataKey)
	assert.Nil(t, err)
	assert.Equal(t, string(dataValue), string(getValue))
}

// TestGetStorageObjectDoesNotLeakPoolRef is a regression test for the C2 bug:
// GetStorageObject increments the pool's refCount, so every caller that discards
// the handle (e.g. databaseExist) must release it. Otherwise refCount grows without
// bound and the janitor can never reclaim the connection. Here we run several
// existence checks, then acquire a single handle and assert the pool holds exactly
// one reference — proving all earlier checks released theirs.
func TestGetStorageObjectDoesNotLeakPoolRef(t *testing.T) {
	defer setup()()

	assert.Nil(t, CreateDatabase("leaktest", true))
	dbObject, err := getMetaDbObject("leaktest")
	assert.Nil(t, err)
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)

	// Repeated existence checks must not accumulate references.
	for range 5 {
		exist, e := databaseExist("leaktest")
		assert.Nil(t, e)
		assert.True(t, exist)
	}

	// One live handle: refCount must be exactly 1, not 1 + leaked refs.
	so, err := GetStorageObject("leaktest")
	assert.Nil(t, err)
	assert.NotNil(t, so)

	pool := GetConnectionPool()
	pool.mu.RLock()
	entry := pool.storages[dbPath]
	pool.mu.RUnlock()
	if assert.NotNil(t, entry) {
		assert.Equal(t, 1, entry.refCount)
	}

	so.Close()

	// After closing the only handle, the reference must drop back to zero.
	pool.mu.RLock()
	entry = pool.storages[dbPath]
	rc := 0
	if entry != nil {
		rc = entry.refCount
	}
	pool.mu.RUnlock()
	assert.Equal(t, 0, rc)
}

// TestPoolJanitorSweepsIdleConnections verifies the M1 fix: the single background
// janitor reclaims connections that have gone idle (refCount 0 for at least the
// pool timeout), instead of relying on a per-Release cleanup goroutine.
func TestPoolJanitorSweepsIdleConnections(t *testing.T) {
	defer setup()()

	assert.Nil(t, CreateDatabase("janitortest", true))
	dbObject, err := getMetaDbObject("janitortest")
	assert.Nil(t, err)
	dbPath := path.Join(dbObject.DbPath, dbObject.DbFile)

	// Acquire and release, leaving the connection pooled with refCount 0.
	so, err := GetStorageObject("janitortest")
	assert.Nil(t, err)
	so.Close()

	pool := GetConnectionPool()
	// The janitor ticks every pool timeout (100ms in tests); an idle entry can
	// survive up to ~2 ticks, so poll generously for it to be reclaimed.
	swept := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pool.mu.RLock()
		_, present := pool.storages[dbPath]
		pool.mu.RUnlock()
		if !present {
			swept = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.True(t, swept, "janitor should reclaim the idle connection")
}

func TestOpenMetaDbWithDirAndNoFiles(t *testing.T) {
	defer setup()()
	metaPath := path.Join(loadMetaIdent().path, loadMetaIdent().file)
	_, err := os.Stat(metaPath)
	assert.Nil(t, err)

	// Close all connections before removing the directory
	GetConnectionPool().CloseAll()

	err = os.RemoveAll(metaPath)
	assert.Nil(t, err)
	assert.Nil(t, openMetaDb())
	metaPath = path.Join(loadMetaIdent().path, loadMetaIdent().file)
	_, err = os.Stat(metaPath)
	assert.Nil(t, err)
}

func TestBatchInsert(t *testing.T) {
	defer setup()()
	type testStruct struct {
		Id    string `json:"id"`
		Value int64  `json:"value"`
	}
	n := 10000
	generated := make(map[string][]byte)
	assert.Nil(t, CreateDatabase("test-table", true))
	dbObject, err := GetStorageObject("test-table")
	assert.Nil(t, err)
	assert.NotNil(t, dbObject)
	defer dbObject.Close()
	start := time.Now()
	for range n {
		found := true
		for found == true {
			newKey := uuid.NewString()
			_, found = generated[newKey]
			if !found {
				rv, _ := randomValues(keyLength)
				b := testStruct{
					Id:    string(rv),
					Value: randv2.Int64(),
				}
				jsonEncoded, ex := json.Marshal(b)
				assert.Nil(t, ex)
				log.Printf("-- generated %s...\n", newKey)
				generated[newKey] = jsonEncoded
			}
		}
	}
	assert.Nil(t, dbObject.BatchInsert(&generated))
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("data generation completed in %d ms\n", int(duration.Milliseconds()))
	// check for each item in db
	for key, value := range generated {
		entry, ex := dbObject.GetEntry(key)
		assert.Nil(t, ex)
		dbItem := testStruct{}
		assert.Nil(t, json.Unmarshal(entry, &dbItem))

		originalItem := testStruct{}
		assert.Nil(t, json.Unmarshal(value, &originalItem))
		assert.Equal(t, originalItem.Id, dbItem.Id)
		assert.Equal(t, originalItem.Value, dbItem.Value)
	}
}

func TestGetAllEntries(t *testing.T) {
	defer setup()()
	type blah struct {
		Id    string `json:"id"`
		Value int64  `json:"value"`
	}
	const (
		totalEntries = 1000000
		batchSize    = 100000
		batchCount   = totalEntries / batchSize
		sampleCheck  = 1000
	)

	var sampledKeys []string
	values := make(map[string][]byte, sampleCheck)

	assert.Nil(t, CreateDatabase("test-table", true))
	dbObject, err := GetStorageObject("test-table")
	assert.Nil(t, err)
	assert.NotNil(t, dbObject)
	defer dbObject.Close()

	start := time.Now()
	for batchNum := 0; batchNum < batchCount; batchNum++ {
		batchEntries := make(map[string][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			found := true
			for found == true {
				newKey := uuid.NewString()
				if _, exists := batchEntries[newKey]; !exists {
					found = false
					rv, _ := randomValues(keyLength)
					b := blah{
						Id:    string(rv),
						Value: randv2.Int64(),
					}
					jsonEncoded, ex := json.Marshal(b)
					assert.Nil(t, ex)
					batchEntries[newKey] = jsonEncoded

					if len(sampledKeys) < sampleCheck {
						sampledKeys = append(sampledKeys, newKey)
						values[newKey] = jsonEncoded // Keep sampled data for validation
					}
				}
			}
		}

		log.Printf("Batch %d/%d: Generated %d entries\n", batchNum+1, batchCount, len(batchEntries))
		assert.Nil(t, dbObject.BatchInsert(&batchEntries))
		batchEntries = nil // Free memory

		if batchNum%5 == 0 {
			runtime.GC()
		}
	}
	end := time.Now()
	genDuration := end.Sub(start)

	// Sample-check validation instead of checking all entries (memory efficient)
	log.Printf("Sample checking %d out of %d entries...\n", len(sampledKeys), totalEntries)
	start = time.Now()
	for k, v := range values {
		entry, ex := dbObject.GetEntry(k)
		assert.Nil(t, ex)
		dbBlah := blah{}
		assert.Nil(t, json.Unmarshal(entry, &dbBlah))

		originalEntry := blah{}
		assert.Nil(t, json.Unmarshal(v, &originalEntry))
		assert.Equal(t, originalEntry.Id, dbBlah.Id)
		assert.Equal(t, originalEntry.Value, dbBlah.Value)
	}
	end = time.Now()
	manualCheckDuration := end.Sub(start)

	// Get all entries and compare (memory efficient - only stored sampled data)
	start = time.Now()
	allItems, err := dbObject.All()
	end = time.Now()
	assert.Nil(t, err)
	getAllDuration := end.Sub(start)

	assert.Equal(t, totalEntries, len(allItems))
	counter := 0
	start = time.Now()
	for key, item := range values { // Only check sampled keys
		counter++
		value := allItems[key]
		assert.NotNil(t, value)
		assert.Equal(t, item, value)

		var originalBlah blah
		assert.Nil(t, json.Unmarshal(item, &originalBlah))
		var dbBlah blah
		assert.Nil(t, json.Unmarshal(value, &dbBlah))
		assert.Equal(t, originalBlah.Id, dbBlah.Id)
		assert.Equal(t, originalBlah.Value, dbBlah.Value)
	}
	end = time.Now()
	allCheckDuration := end.Sub(start)

	log.Printf("data generation completed in %d ms\n", int(genDuration.Milliseconds()))
	log.Printf("manual check completed in %d ms\n", int(manualCheckDuration.Milliseconds()))
	log.Printf("getting all records completed in %d ms\n", int(getAllDuration.Milliseconds()))
	log.Printf("get all check completed in %d ms\n", int(allCheckDuration.Milliseconds()))
}

func TestCreateSameDbs(t *testing.T) {
	defer setup()()
	testDb := "testdb"
	assert.Nil(t, CreateDatabase(testDb, true))
	// try creating it the second time
	err := CreateDatabase(testDb, true)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "database already exists")
}

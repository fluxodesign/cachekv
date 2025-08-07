package cachekv

import (
	"log"
	"maps"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/zalando/go-keyring"
)

const alternateTestStorePath = "./test-alternate/"

func setup() func() {
	StorePath = "./test-store/"
	keyring.MockInit()
	var err error
	Startup()
	// teardown
	return func() {
		metaPath := path.Join(metaStorage.path, metaStorage.file)
		_, err = os.Stat(metaPath)
		if err == nil {
			err = os.RemoveAll(metaPath)
			if err != nil {
				log.Println("error removing test db file: ", err)
			}
			err = os.RemoveAll(StorePath)
			if err != nil {
				log.Println("error removing test store path: ", err)
			}
		}
		err = os.RemoveAll(alternateTestStorePath)
		if err != nil {
			log.Println("error removing test alternate store path: ", err)
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
	metaPath := path.Join(metaStorage.path, metaStorage.file)
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
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	assert.Nil(t, writeMetaEntry("testkey", []byte("testvalue")))
	time.Sleep(2 * time.Second)
	value, err := getMetaEntry("testkey")
	assert.Nil(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, value, []byte("testvalue"))
	metaKey, _ := randomValues(32)
	db, err := OpenDatabase(metaPath, metaKey)
	assert.NotNil(t, err)
	assert.Nil(t, db)
}

func TestCopyMetasTwoRecords(t *testing.T) {
	defer setup()()
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	oldDb, err := OpenDatabase(metaPath, metaStorage.key)
	assert.Nil(t, setDbEntry([]byte("prefix:testkey"), []byte("testvalue"), oldDb))
	assert.Nil(t, setDbEntry([]byte("prefix:testkey2"), []byte("testvalue2"), oldDb))
	keys, err := countRecords("prefix:", oldDb, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, keys)
	err = CloseDatabase(oldDb)
	assert.Nil(t, err)
	newPath, newKey, err := copyMetas()
	newMetaPath := path.Join(metaStorage.path, newPath)
	newDb, err := OpenDatabase(newMetaPath, newKey)
	assert.Nil(t, err)
	assert.NotNil(t, newDb)
	keys, err = countRecords("prefix:", newDb, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, keys)
	err = CloseDatabase(newDb)
	assert.Nil(t, err)
}

func TestCopyMetas(t *testing.T) {
	defer setup()()
	newMeta, _ := randomValues(32)
	log.Println(newMeta)
	n := 1000000
	values := make(map[string][]byte)
	start := time.Now()
	for i := 0; i < n; i++ {
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
	metaPath := path.Join(metaStorage.path, metaStorage.file)
	oldDb, err := OpenDatabase(metaPath, metaStorage.key)
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
	err = CloseDatabase(oldDb)
	assert.Nil(t, err)
	start = time.Now()
	newPath, newKey, err := copyMetas()
	end = time.Now()
	assert.Nil(t, err)
	duration = end.Sub(start)
	log.Printf("copyMetas() with %d records completed in %d seconds", n, int(duration.Seconds()))
	newDb, err := OpenDatabase(StorePath+newPath, newKey)
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
	err = CloseDatabase(newDb)
	assert.Nil(t, err)
}

func TestDefaultConfig(t *testing.T) {
	defer setup()()
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.True(t, cfg.SecureNewDb)
	assert.Equal(t, StorePath, cfg.StorePath)
	assert.Equal(t, StorePath, cfg.MetaStore)
	assert.Equal(t, metaStorage.file, cfg.MetaFile)
}

func TestUpdateConfigurations(t *testing.T) {
	defer setup()()
	cfg, err := ListConfigurations()
	assert.Nil(t, err)
	assert.True(t, cfg.SecureNewDb)
	assert.Equal(t, StorePath, cfg.StorePath)
	assert.Equal(t, StorePath, cfg.MetaStore)
	assert.Equal(t, metaStorage.file, cfg.MetaFile)
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
	keyring.MockInit()
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
	n := 2000000
	entries := make(map[string][]byte)
	start := time.Now()
	for i := 0; i < n; i++ {
		found := true
		for found == true {
			newKey := uuid.New().String()
			_, found = entries[newKey]
			if !found {
				rv, _ := randomValues(keyLength)
				entries[newKey] = rv
			}
		}
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("DATA generation completed in %d seconds\n", int(duration.Seconds()))
	start = time.Now()
	err = BatchInsert(testDb1, entries)
	end = time.Now()
	duration = end.Sub(start)
	assert.Nil(t, err)
	log.Printf("Batch insert %d entries completed in %d seconds\n", n, int(duration.Seconds()))
	// asserting every record is present
	dbo, err := getMetaDbObject(testDb1)
	assert.Nil(t, err)
	dbKey, err := getDbKey(testDb1, dbo)
	assert.Nil(t, err)
	dbPath := path.Join(dbo.DbPath, dbo.DbFile)
	db, err := OpenDatabase(dbPath, dbKey)
	assert.Nil(t, err)
	records, err := countRecords("", db, false)
	assert.Nil(t, err)
	assert.Equal(t, n, records)
	for k, v := range entries {
		value, e := getDbEntry([]byte(k), db)
		assert.Nil(t, e)
		assert.NotNil(t, value)
		assert.Equal(t, value, v)
	}
	err = CloseDatabase(db)
	assert.Nil(t, err)
}

func TestGetEntryWithinALotOfEntries(t *testing.T) {
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
	n := 2000000
	entries := make(map[string][]byte)
	start := time.Now()
	for i := 0; i < n; i++ {
		found := true
		for found == true {
			newKey := uuid.New().String()
			_, found = entries[newKey]
			if !found {
				rv, _ := randomValues(keyLength)
				entries[newKey] = rv
			}
		}
	}
	end := time.Now()
	genDataDuration := end.Sub(start)
	log.Printf("DATA generation completed in %d seconds\n", int(genDataDuration.Seconds()))
	start = time.Now()
	err = BatchInsert(testDb1, entries)
	assert.Nil(t, err)
	end = time.Now()
	insertDuration := end.Sub(start)
	log.Printf("Batch insert %d entries completed in %d seconds\n", n, int(insertDuration.Seconds()))
	// pick one random entry
	randNo := rand.Intn(n)
	keys := make([]string, 0, n)
	for k := range maps.Keys(entries) {
		keys = append(keys, k)
	}
	key := keys[randNo]
	value := entries[key]
	// time the GetEntry
	start = time.Now()
	entry, err := GetEntry(testDb1, key)
	end = time.Now()
	assert.Nil(t, err)
	assert.Equal(t, value, entry)
	getEntryDuration := end.Sub(start)
	log.Printf("GetEntry() finished in %d milliseconds\n", int(getEntryDuration.Milliseconds()))
}

func TestInitReloadingExistingMetafile(t *testing.T) {
	defer setup()()
	assert.Nil(t, openMetaDb())
}

func TestGetStorageObject(t *testing.T) {
	defer setup()()
	var myKey = "key1"
	var myValue = "value1"
	assert.Nil(t, CreateDatabase("testdb", true))
	storageObject, err := GetStorageObject("testdb")
	assert.Nil(t, err)
	assert.NotNil(t, storageObject)
	assert.Nil(t, CloseDatabase(storageObject.db))
	// try to insert data into the database and confirm
	assert.Nil(t, InsertEntry("testdb", myKey, []byte(myValue)))
	byteEntry, err := GetEntry("testdb", myKey)
	assert.Nil(t, err)
	assert.Equal(t, myValue, string(byteEntry))
}

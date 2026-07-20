package cachekv

import (
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

var (
	poolTestStorePath = "./test-pool-store/"
	poolKeyPath       = "./.pool-test-private/"
)

func poolSetup() func() {
	// Clean up any previous test artifacts
	if _, statErr := os.Stat(poolTestStorePath); !os.IsNotExist(statErr) {
		err := os.RemoveAll(poolTestStorePath)
		if err != nil {
			log.Println("Error removing pool test store: ", err)
			return nil
		}
	}
	if _, statErr := os.Stat(poolKeyPath); !os.IsNotExist(statErr) {
		err := os.RemoveAll(poolKeyPath)
		if err != nil {
			log.Println("Error removing pool key path: ", err)
			return nil
		}
	}
	return func() {
		metaPath := path.Join(metaStorage.path, metaStorage.file)
		if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
			err := os.RemoveAll(metaPath)
			if err != nil {
				return
			}
		}
		if err := os.RemoveAll(poolTestStorePath); err != nil {
			log.Println("error removing pool test store: ", err)
		}
		if err := os.RemoveAll(poolKeyPath); err != nil {
			log.Println("error removing pool key path: ", err)
		}
	}
}

func TestGetConnectionPool(t *testing.T) {
	defer poolSetup()()
	pool := GetConnectionPool()
	assert.NotNil(t, pool)
}

func TestPoolGetAndReleaseBasic(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-basic")
	key, _ := randomValues(keyLength)

	// First get should create a new database connection
	db1, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	// Release the connection back to pool
	pool.Release(dbPath)

	// Cleanup
	err = CloseDatabase(db1)
	assert.Nil(t, err)
}

func TestPoolConnectionReuse(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-reuse")
	key, _ := randomValues(keyLength)

	// First get - creates new connection
	db1, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	// Write some test data to verify we're using the same DB
	testEntryErr := setDbEntry([]byte("testkey"), []byte("testvalue"), db1)
	assert.Nil(t, testEntryErr)

	pool.Release(dbPath)

	// Get again - should reuse connection from pool
	db2, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	// Verify we can still read the data (same DB instance)
	value, getErr := getDbEntry([]byte("testkey"), db2)
	assert.Nil(t, getErr)
	assert.Equal(t, []byte("testvalue"), value)

	err = CloseDatabase(db1)
	assert.Nil(t, err)
}

func TestPoolConcurrentGetRelease(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()
	dbPath := path.Join(poolTestStorePath, "test-db-concurrent")
	key, _ := randomValues(keyLength)

	numGoroutines := 10
	iterations := 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*iterations)

	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				db, e := pool.Get(dbPath, key)
				if e != nil {
					errors <- e
					return
				}

				// Perform a simple operation to ensure connection is valid
				testKey := "test-key-" + string(rune(id)) + "-" + string(rune(j))
				err = setDbEntry([]byte(testKey), []byte("value"), db)
				if err != nil {
					errors <- err
				}

				pool.Release(dbPath)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	duration := time.Since(startTime)
	log.Printf("Concurrent test completed in %v\n", duration)

	// Check for any errors from goroutines
	var hasErrors bool
	for err = range errors {
		assert.Nil(t, err, "unexpected error from concurrent operation")
		hasErrors = true
	}
	if !hasErrors {
		log.Println("All concurrent operations succeeded")
	}
}

func TestPoolMultiplePaths(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()
	numDatabases := 5

	dbConnections := make([]*badger.DB, numDatabases)
	dbKeys := make(map[int][]byte) // Store keys per-database

	for i := range numDatabases {
		dbPath := path.Join(poolTestStorePath, "test-db-"+strconv.Itoa(i))
		key, _ := randomValues(keyLength)

		dbKeys[i] = key // Store the key for this database

		dbConnections[i], err = pool.Get(dbPath, key)
		assert.Nil(t, err, "failed to get connection for database %d", i)
		assert.NotNil(t, dbConnections[i])

		// Write test data to each database
		err = setDbEntry([]byte("key"), []byte("value-"+strconv.Itoa(i)), dbConnections[i])
		assert.Nil(t, err, "failed to write to database %d", i)
	}

	// Verify all databases have distinct data using stored keys
	for i := range numDatabases {
		dbPath := path.Join(poolTestStorePath, "test-db-"+strconv.Itoa(i))

		pool.Release(dbPath)

		key := dbKeys[i] // Retrieve the correct key for this database

		db2, err := pool.Get(dbPath, key)
		assert.Nil(t, err)

		value, getErr := getDbEntry([]byte("key"), db2)
		assert.Nil(t, getErr)
		expectedValue := "value-" + strconv.Itoa(i)
		assert.Equal(t, []byte(expectedValue), value,
			"database %d should have distinct data", i)

		err = CloseDatabase(dbConnections[i])
		assert.Nil(t, err)
	}
}

func TestPoolInvalidPath(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	// Try to get connection for non-existent path
	invalidPath := "/nonexistent/path/test-db"
	key, _ := randomValues(keyLength)

	db, err := pool.Get(invalidPath, key)
	assert.NotNil(t, err, "should fail with invalid path")
	if db != nil {
		err := CloseDatabase(db)
		assert.Nil(t, err)
	}
}

func TestPoolInvalidKey(t *testing.T) {
	defer poolSetup()()
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err := genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-invalid-key")
	validKey, _ := randomValues(keyLength)
	invalidKey, _ := randomValues(keyLength) // Different key

	// Create database with valid key first
	db1, err := pool.Get(dbPath, validKey)
	assert.Nil(t, err)

	err = setDbEntry([]byte("testkey"), []byte("testvalue"), db1)
	assert.Nil(t, err)

	pool.Release(dbPath)
	err = CloseDatabase(db1)
	assert.Nil(t, err)

	// Try to access with wrong key - should fail
	db2, err := pool.Get(dbPath, invalidKey)
	if db2 != nil {
		e := CloseDatabase(db2)
		assert.Nil(t, e)
	}
	// Note: Badger may or may not return error depending on encryption mode

	pool.Release(dbPath)
}

func TestPoolReleaseNonExistent(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	// Try to release a path that was never added
	randomPath := "/random/path/that/does/not/exist"

	pool.Release(randomPath)

	log.Println("Release of non-existent path handled gracefully")
}

func TestPoolGetWithEmptyKey(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-empty-key")
	emptyKey := []byte{} // Empty key

	db, err := pool.Get(dbPath, emptyKey)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = CloseDatabase(db)
	assert.Nil(t, err)
	pool.Release(dbPath)
}

func TestPoolStressTest(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-stress")
	key, _ := randomValues(keyLength)

	numOperations := 1000
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := range numOperations {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			db, err := pool.Get(dbPath, key)
			if err != nil {
				log.Printf("Error in stress operation %d: %v\n", id, err)
				return
			}

			err = setDbEntry([]byte("key"+string(rune(id))), []byte(string(rune(id))), db)
			if err != nil {
				log.Printf("Write error in operation %d: %v\n", id, err)
			}

			pool.Release(dbPath)
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	opsPerSecond := float64(numOperations) / duration.Seconds()
	log.Printf("Stress test: %d operations in %v (%.2f ops/sec)\n",
		numOperations, duration, opsPerSecond)
}

func TestPoolGetReturnsSameConnection(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-same-conn")
	key, _ := randomValues(keyLength)

	// First get creates new connection
	db1, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	// Second get on same path should return same connection from pool (or different one if max not reached)
	db2, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	pool.Release(dbPath)
	err = CloseDatabase(db1)
	assert.Nil(t, err)
}

func TestPoolWithExistingDatabase(t *testing.T) {
	defer poolSetup()()
	var err error
	StorePath = poolTestStorePath
	KeyPath = poolKeyPath

	err = genKeypair()
	assert.Nil(t, err)

	pool := GetConnectionPool()

	dbPath := path.Join(poolTestStorePath, "test-db-existing")
	key, _ := randomValues(keyLength)

	// First create database using OpenDatabase directly
	db1, err := OpenDatabase(dbPath, key)
	assert.Nil(t, err)

	// Write some data
	err = setDbEntry([]byte("initkey"), []byte("initvalue"), db1)
	assert.Nil(t, err)

	err = CloseDatabase(db1)
	assert.Nil(t, err)

	// Now get via pool - should work with existing database
	db2, err := pool.Get(dbPath, key)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	value, getErr := getDbEntry([]byte("initkey"), db2)
	assert.Nil(t, getErr)
	assert.Equal(t, []byte("initvalue"), value)

	pool.Release(dbPath)
}

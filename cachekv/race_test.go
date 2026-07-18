package cachekv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrentGlobalStateAccess(t *testing.T) {
	defer setup()()

	dbName := "race-test-db"
	err := CreateDatabase(dbName, true)
	assert.Nil(t, err)

	numGoroutines := 100
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	results := make(chan struct {
		key   string
		value []byte
	}, numGoroutines)

	startTime := time.Now()

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			key := "key" + string(rune(id))
			value := []byte("value")

			err := InsertEntry(dbName, key, value)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d insert error: %w", id, err)
				return
			}

			getValue, getErr := GetEntry(dbName, key)
			if getErr != nil {
				errors <- fmt.Errorf("goroutine %d get error: %w", id, err)
				return
			}

			results <- struct {
				key   string
				value []byte
			}{key, getValue}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(errors)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out after 30 seconds - possible deadlock")
	}

	close(errors)
	close(results)

	duration := time.Since(startTime)

	var hasErrors bool
	for err = range errors {
		hasErrors = true
		t.Errorf("Error in concurrent operation: %v", err)
	}

	for result := range results {
		expectedValue := []byte("value")
		if !bytes.Equal(result.value, expectedValue) {
			hasErrors = true
			t.Errorf("Assertion failed for key %s: got %v, want %v",
				result.key, string(result.value), string(expectedValue))
		}
	}

	assert.False(t, hasErrors, "no race conditions should occur")
	log.Printf("Concurrent test completed in %v\n", duration)
}

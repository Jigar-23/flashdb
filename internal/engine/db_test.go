package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDBBasics(t *testing.T) {
	db := NewDB(Config{})
	defer db.Close()

	// SET and GET
	err := db.Set("name", "FlashDB", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, ok, err := db.Get("name")
	if err != nil || !ok || val != "FlashDB" {
		t.Fatalf("Get mismatch: val=%v, ok=%v, err=%v", val, ok, err)
	}

	// EXISTS
	if !db.Exists("name") {
		t.Fatalf("expected name to exist")
	}

	// DEL
	if !db.Del("name") {
		t.Fatalf("expected Del to succeed")
	}
	if db.Exists("name") {
		t.Fatalf("expected name to be deleted")
	}
}

func TestDBTTL(t *testing.T) {
	db := NewDB(Config{})
	defer db.Close()

	// Set with 50ms TTL
	err := db.Set("temp", "value", 50)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if !db.Exists("temp") {
		t.Fatalf("temp should exist initially")
	}

	// Wait 70ms for passive expiry
	time.Sleep(70 * time.Millisecond)

	if db.Exists("temp") {
		t.Fatalf("temp should have expired")
	}

	_, ok, _ := db.Get("temp")
	if ok {
		t.Fatalf("Get should return not found after expiration")
	}
}

func TestDBIncr(t *testing.T) {
	db := NewDB(Config{})
	defer db.Close()

	res, err := db.Incr("counter", 1)
	if err != nil || res != 1 {
		t.Fatalf("Incr failed: res=%v, err=%v", res, err)
	}

	res, err = db.Incr("counter", 5)
	if err != nil || res != 6 {
		t.Fatalf("Incr failed: res=%v, err=%v", res, err)
	}
}

func TestDBCollections(t *testing.T) {
	db := NewDB(Config{})
	defer db.Close()

	// Lists
	count, err := db.RPush("mylist", "a", "b", "c")
	if err != nil || count != 3 {
		t.Fatalf("RPush failed: count=%v, err=%v", count, err)
	}
	item, ok, err := db.LPop("mylist")
	if err != nil || !ok || item != "a" {
		t.Fatalf("LPop failed: item=%v", item)
	}

	// Hashes
	hsetRes, err := db.HSet("user:1", "email", "test@faang.com")
	if err != nil || hsetRes != 1 {
		t.Fatalf("HSet failed: %v", err)
	}
	val, ok, err := db.HGet("user:1", "email")
	if err != nil || !ok || val != "test@faang.com" {
		t.Fatalf("HGet failed: val=%v", val)
	}

	// Sets
	saddRes, err := db.SAdd("myset", "apple", "banana")
	if err != nil || saddRes != 2 {
		t.Fatalf("SAdd failed: %v", err)
	}
	isMem, err := db.SIsMember("myset", "apple")
	if err != nil || !isMem {
		t.Fatalf("SIsMember failed: %v", err)
	}
}

func TestDBConcurrentStress(t *testing.T) {
	db := NewDB(Config{})
	defer db.Close()

	const numWorkers = 50
	const opsPerWorker = 200

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker:%d:key:%d", workerID, i)
				val := fmt.Sprintf("val:%d", i)
				if err := db.Set(key, val, 0); err != nil {
					t.Errorf("Set error: %v", err)
				}
				got, ok, err := db.Get(key)
				if err != nil || !ok || got != val {
					t.Errorf("Get mismatch for %s: got=%s, want=%s", key, got, val)
				}
			}
		}()
	}

	wg.Wait()
}

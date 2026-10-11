package eviction

import (
	"testing"
)

func TestLRUEvictionOrder(t *testing.T) {
	lru := NewLRU(3)

	lru.Access("key1")
	lru.Access("key2")
	lru.Access("key3")

	// Access key1 again so key2 becomes the oldest (LRU)
	lru.Access("key1")

	// Evict first should be key2
	evicted, ok := lru.Evict()
	if !ok || evicted != "key2" {
		t.Fatalf("expected key2 to be evicted, got %q, ok=%v", evicted, ok)
	}

	// Evict second should be key3
	evicted, ok = lru.Evict()
	if !ok || evicted != "key3" {
		t.Fatalf("expected key3 to be evicted, got %q, ok=%v", evicted, ok)
	}

	// Evict third should be key1
	evicted, ok = lru.Evict()
	if !ok || evicted != "key1" {
		t.Fatalf("expected key1 to be evicted, got %q, ok=%v", evicted, ok)
	}

	// Evict fourth should return false (empty)
	_, ok = lru.Evict()
	if ok {
		t.Fatalf("expected no more evictions")
	}
}

func TestLRURemove(t *testing.T) {
	lru := NewLRU(5)
	lru.Access("a")
	lru.Access("b")
	lru.Access("c")

	lru.Remove("b")

	evicted, ok := lru.Evict()
	if !ok || evicted != "a" {
		t.Fatalf("expected 'a' after removing 'b', got %q", evicted)
	}

	evicted, ok = lru.Evict()
	if !ok || evicted != "c" {
		t.Fatalf("expected 'c', got %q", evicted)
	}
}

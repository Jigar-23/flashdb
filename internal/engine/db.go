package engine

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jigar-23/flashdb/internal/eviction"
)

const DefaultShards = 64

var (
	ErrOOM          = errors.New("OOM command not allowed when used memory > 'maxmemory'")
	ErrWrongType    = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotAnInteger = errors.New("ERR value is not an integer or out of range")
)

type Shard struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

type Config struct {
	MaxMemory      int64 // 0 for unlimited
	EvictionPolicy string
}

type DB struct {
	shards    [DefaultShards]*Shard
	lru       *eviction.LRU
	config    Config
	usedMem   int64 // atomic byte counter
	keysCount int64 // atomic count of keys
	stopChan  chan struct{}
}

// FNV-1a 32-bit hash
func fnv32(key string) uint32 {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

func (db *DB) getShard(key string) *Shard {
	idx := fnv32(key) % DefaultShards
	return db.shards[idx]
}

// NewDB initializes a sharded FlashDB database
func NewDB(cfg Config) *DB {
	db := &DB{
		config:   cfg,
		lru:      eviction.NewLRU(0),
		stopChan: make(chan struct{}),
	}
	for i := 0; i < DefaultShards; i++ {
		db.shards[i] = &Shard{
			entries: make(map[string]*Entry),
		}
	}

	// Start background active TTL expiration sweeper
	go db.activeExpiryLoop()

	return db
}

// Close shuts down background workers
func (db *DB) Close() {
	close(db.stopChan)
}

// Memory tracking
func (db *DB) UsedMemory() int64 {
	return atomic.LoadInt64(&db.usedMem)
}

func (db *DB) KeysCount() int64 {
	return atomic.LoadInt64(&db.keysCount)
}

func (db *DB) checkEviction(neededBytes int64) error {
	if db.config.MaxMemory <= 0 {
		return nil
	}

	for atomic.LoadInt64(&db.usedMem)+neededBytes > db.config.MaxMemory {
		evictedKey, ok := db.lru.Evict()
		if !ok {
			if atomic.LoadInt64(&db.usedMem)+neededBytes > db.config.MaxMemory {
				return ErrOOM
			}
			break
		}
		db.Del(evictedKey)
	}
	return nil
}

// Set stores a string key-value with optional TTL in milliseconds
func (db *DB) Set(key, val string, ttlMillis int64) error {
	entry := NewStringEntry(val)
	if ttlMillis > 0 {
		entry.ExpiresAt = time.Now().UnixMilli() + ttlMillis
	}

	if err := db.checkEviction(entry.Size); err != nil {
		return err
	}

	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	old, exists := shard.entries[key]
	if exists {
		atomic.AddInt64(&db.usedMem, entry.Size-old.Size)
	} else {
		atomic.AddInt64(&db.usedMem, entry.Size)
		atomic.AddInt64(&db.keysCount, 1)
	}

	shard.entries[key] = entry
	db.lru.Access(key)
	return nil
}

// Get retrieves a string value, checking passive expiration
func (db *DB) Get(key string) (string, bool, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if !exists {
		return "", false, nil
	}

	if entry.IsExpired(now) {
		// Passive expiration
		db.Del(key)
		return "", false, nil
	}

	if entry.Type != TypeString {
		return "", false, ErrWrongType
	}

	// Update access for LRU
	entry.LastAccessed = now
	atomic.AddInt64(&entry.AccessCount, 1)
	db.lru.Access(key)

	return entry.Value.(string), true, nil
}

// Del removes a key from database
func (db *DB) Del(key string) bool {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	if !exists {
		return false
	}

	delete(shard.entries, key)
	atomic.AddInt64(&db.usedMem, -entry.Size)
	atomic.AddInt64(&db.keysCount, -1)
	db.lru.Remove(key)
	return true
}

// Exists checks if key exists and has not expired
func (db *DB) Exists(key string) bool {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if !exists {
		return false
	}

	if entry.IsExpired(now) {
		db.Del(key)
		return false
	}
	return true
}

// Expire sets a key's TTL in milliseconds
func (db *DB) Expire(key string, ttlMillis int64) bool {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return false
	}

	entry.ExpiresAt = now + ttlMillis
	return true
}

// TTL returns remaining TTL in seconds (-1 for no expiry, -2 for non-existent)
func (db *DB) TTL(key string) int64 {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return -2
	}

	if entry.ExpiresAt == 0 {
		return -1
	}

	remaining := (entry.ExpiresAt - now) / 1000
	if remaining < 0 {
		return -2
	}
	return remaining
}

// PTTL returns remaining TTL in milliseconds
func (db *DB) PTTL(key string) int64 {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return -2
	}

	if entry.ExpiresAt == 0 {
		return -1
	}

	remaining := entry.ExpiresAt - now
	if remaining < 0 {
		return -2
	}
	return remaining
}

// Persist removes TTL from a key
func (db *DB) Persist(key string) bool {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) || entry.ExpiresAt == 0 {
		return false
	}

	entry.ExpiresAt = 0
	return true
}

// Incr increments integer value by delta
func (db *DB) Incr(key string, delta int64) (int64, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		newVal := delta
		strVal := strconv.FormatInt(newVal, 10)
		newEntry := NewStringEntry(strVal)
		shard.entries[key] = newEntry
		atomic.AddInt64(&db.usedMem, newEntry.Size)
		atomic.AddInt64(&db.keysCount, 1)
		db.lru.Access(key)
		return newVal, nil
	}

	if entry.Type != TypeString {
		return 0, ErrWrongType
	}

	valStr := entry.Value.(string)
	current, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, ErrNotAnInteger
	}

	newVal := current + delta
	newStr := strconv.FormatInt(newVal, 10)
	oldSize := entry.Size
	entry.Value = newStr
	entry.Size = int64(len(newStr) + 64)
	atomic.AddInt64(&db.usedMem, entry.Size-oldSize)

	db.lru.Access(key)
	return newVal, nil
}

// --- List Operations ---

func (db *DB) LPush(key string, values ...string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		list := make([]string, 0, len(values))
		for i := len(values) - 1; i >= 0; i-- {
			list = append(list, values[i])
		}
		newEntry := &Entry{
			Type:         TypeList,
			Value:        list,
			CreatedAt:    now,
			LastAccessed: now,
			AccessCount:  1,
			Size:         int64(len(values)*32 + 64),
		}
		shard.entries[key] = newEntry
		atomic.AddInt64(&db.usedMem, newEntry.Size)
		atomic.AddInt64(&db.keysCount, 1)
		db.lru.Access(key)
		return len(list), nil
	}

	if entry.Type != TypeList {
		return 0, ErrWrongType
	}

	list := entry.Value.([]string)
	for _, v := range values {
		list = append([]string{v}, list...)
	}
	entry.Value = list
	entry.Size += int64(len(values) * 32)
	atomic.AddInt64(&db.usedMem, int64(len(values)*32))
	db.lru.Access(key)
	return len(list), nil
}

func (db *DB) RPush(key string, values ...string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		list := append([]string{}, values...)
		newEntry := &Entry{
			Type:         TypeList,
			Value:        list,
			CreatedAt:    now,
			LastAccessed: now,
			AccessCount:  1,
			Size:         int64(len(values)*32 + 64),
		}
		shard.entries[key] = newEntry
		atomic.AddInt64(&db.usedMem, newEntry.Size)
		atomic.AddInt64(&db.keysCount, 1)
		db.lru.Access(key)
		return len(list), nil
	}

	if entry.Type != TypeList {
		return 0, ErrWrongType
	}

	list := entry.Value.([]string)
	list = append(list, values...)
	entry.Value = list
	entry.Size += int64(len(values) * 32)
	atomic.AddInt64(&db.usedMem, int64(len(values)*32))
	db.lru.Access(key)
	return len(list), nil
}

func (db *DB) LPop(key string) (string, bool, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		return "", false, nil
	}
	if entry.Type != TypeList {
		return "", false, ErrWrongType
	}

	list := entry.Value.([]string)
	if len(list) == 0 {
		return "", false, nil
	}

	item := list[0]
	entry.Value = list[1:]
	if len(list[1:]) == 0 {
		delete(shard.entries, key)
		atomic.AddInt64(&db.usedMem, -entry.Size)
		atomic.AddInt64(&db.keysCount, -1)
		db.lru.Remove(key)
	}
	return item, true, nil
}

func (db *DB) RPop(key string) (string, bool, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		return "", false, nil
	}
	if entry.Type != TypeList {
		return "", false, ErrWrongType
	}

	list := entry.Value.([]string)
	n := len(list)
	if n == 0 {
		return "", false, nil
	}

	item := list[n-1]
	entry.Value = list[:n-1]
	if n-1 == 0 {
		delete(shard.entries, key)
		atomic.AddInt64(&db.usedMem, -entry.Size)
		atomic.AddInt64(&db.keysCount, -1)
		db.lru.Remove(key)
	}
	return item, true, nil
}

func (db *DB) LRange(key string, start, stop int) ([]string, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return []string{}, nil
	}
	if entry.Type != TypeList {
		return nil, ErrWrongType
	}

	list := entry.Value.([]string)
	n := len(list)
	if n == 0 {
		return []string{}, nil
	}

	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if start >= n {
		return []string{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}, nil
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result, nil
}

// --- Hash Operations ---

func (db *DB) HSet(key, field, val string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()

	if !exists || entry.IsExpired(now) {
		hmap := map[string]string{field: val}
		newEntry := &Entry{
			Type:         TypeHash,
			Value:        hmap,
			CreatedAt:    now,
			LastAccessed: now,
			AccessCount:  1,
			Size:         int64(len(field) + len(val) + 64),
		}
		shard.entries[key] = newEntry
		atomic.AddInt64(&db.usedMem, newEntry.Size)
		atomic.AddInt64(&db.keysCount, 1)
		db.lru.Access(key)
		return 1, nil
	}

	if entry.Type != TypeHash {
		return 0, ErrWrongType
	}

	hmap := entry.Value.(map[string]string)
	_, fieldExists := hmap[field]
	hmap[field] = val

	diff := int64(len(val))
	if !fieldExists {
		diff += int64(len(field) + 32)
	}
	entry.Size += diff
	atomic.AddInt64(&db.usedMem, diff)
	db.lru.Access(key)

	if fieldExists {
		return 0, nil
	}
	return 1, nil
}

func (db *DB) HGet(key, field string) (string, bool, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return "", false, nil
	}
	if entry.Type != TypeHash {
		return "", false, ErrWrongType
	}

	hmap := entry.Value.(map[string]string)
	val, ok := hmap[field]
	return val, ok, nil
}

func (db *DB) HDel(key string, fields ...string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		return 0, nil
	}
	if entry.Type != TypeHash {
		return 0, ErrWrongType
	}

	hmap := entry.Value.(map[string]string)
	deleted := 0
	for _, f := range fields {
		if _, ok := hmap[f]; ok {
			delete(hmap, f)
			deleted++
		}
	}
	if len(hmap) == 0 {
		delete(shard.entries, key)
		atomic.AddInt64(&db.usedMem, -entry.Size)
		atomic.AddInt64(&db.keysCount, -1)
		db.lru.Remove(key)
	}
	return deleted, nil
}

func (db *DB) HGetAll(key string) (map[string]string, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return map[string]string{}, nil
	}
	if entry.Type != TypeHash {
		return nil, ErrWrongType
	}

	hmap := entry.Value.(map[string]string)
	res := make(map[string]string, len(hmap))
	for k, v := range hmap {
		res[k] = v
	}
	return res, nil
}

// --- Set Operations ---

func (db *DB) SAdd(key string, members ...string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()

	if !exists || entry.IsExpired(now) {
		set := make(map[string]struct{})
		for _, m := range members {
			set[m] = struct{}{}
		}
		newEntry := &Entry{
			Type:         TypeSet,
			Value:        set,
			CreatedAt:    now,
			LastAccessed: now,
			AccessCount:  1,
			Size:         int64(len(members)*32 + 64),
		}
		shard.entries[key] = newEntry
		atomic.AddInt64(&db.usedMem, newEntry.Size)
		atomic.AddInt64(&db.keysCount, 1)
		db.lru.Access(key)
		return len(set), nil
	}

	if entry.Type != TypeSet {
		return 0, ErrWrongType
	}

	set := entry.Value.(map[string]struct{})
	added := 0
	for _, m := range members {
		if _, ok := set[m]; !ok {
			set[m] = struct{}{}
			added++
		}
	}
	if added > 0 {
		diff := int64(added * 32)
		entry.Size += diff
		atomic.AddInt64(&db.usedMem, diff)
	}
	db.lru.Access(key)
	return added, nil
}

func (db *DB) SMembers(key string) ([]string, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return []string{}, nil
	}
	if entry.Type != TypeSet {
		return nil, ErrWrongType
	}

	set := entry.Value.(map[string]struct{})
	members := make([]string, 0, len(set))
	for m := range set {
		members = append(members, m)
	}
	return members, nil
}

func (db *DB) SRem(key string, members ...string) (int, error) {
	shard := db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[key]
	now := time.Now().UnixMilli()
	if !exists || entry.IsExpired(now) {
		return 0, nil
	}
	if entry.Type != TypeSet {
		return 0, ErrWrongType
	}

	set := entry.Value.(map[string]struct{})
	removed := 0
	for _, m := range members {
		if _, ok := set[m]; ok {
			delete(set, m)
			removed++
		}
	}
	if len(set) == 0 {
		delete(shard.entries, key)
		atomic.AddInt64(&db.usedMem, -entry.Size)
		atomic.AddInt64(&db.keysCount, -1)
		db.lru.Remove(key)
	} else if removed > 0 {
		diff := int64(removed * 32)
		entry.Size -= diff
		atomic.AddInt64(&db.usedMem, -diff)
	}
	return removed, nil
}

func (db *DB) SIsMember(key, member string) (bool, error) {
	shard := db.getShard(key)
	now := time.Now().UnixMilli()

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[key]
	if !exists || entry.IsExpired(now) {
		return false, nil
	}
	if entry.Type != TypeSet {
		return false, ErrWrongType
	}

	set := entry.Value.(map[string]struct{})
	_, ok := set[member]
	return ok, nil
}

// FlushDB empties the database
func (db *DB) FlushDB() {
	for i := 0; i < DefaultShards; i++ {
		db.shards[i].mu.Lock()
		db.shards[i].entries = make(map[string]*Entry)
		db.shards[i].mu.Unlock()
	}
	atomic.StoreInt64(&db.usedMem, 0)
	atomic.StoreInt64(&db.keysCount, 0)
	db.lru = eviction.NewLRU(0)
}

// Active TTL Background Expiration Sweeper (Redis probabilistic sampling)
func (db *DB) activeExpiryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-db.stopChan:
			return
		case <-ticker.C:
			db.activeExpireSample()
		}
	}
}

func (db *DB) activeExpireSample() {
	now := time.Now().UnixMilli()
	// Sample up to 20 keys per random shard
	shardIdx := rand.Intn(DefaultShards)
	shard := db.shards[shardIdx]

	shard.mu.Lock()
	keysToDelete := make([]string, 0)
	sampled := 0
	for k, e := range shard.entries {
		if sampled >= 20 {
			break
		}
		if e.ExpiresAt > 0 {
			sampled++
			if e.IsExpired(now) {
				keysToDelete = append(keysToDelete, k)
			}
		}
	}

	for _, k := range keysToDelete {
		e := shard.entries[k]
		delete(shard.entries, k)
		atomic.AddInt64(&db.usedMem, -e.Size)
		atomic.AddInt64(&db.keysCount, -1)
		db.lru.Remove(k)
	}
	shard.mu.Unlock()
}

// Snapshot returns a copy of all non-expired key-values for AOF rewrite/compaction
func (db *DB) Snapshot() map[string]*Entry {
	now := time.Now().UnixMilli()
	result := make(map[string]*Entry)

	for i := 0; i < DefaultShards; i++ {
		db.shards[i].mu.RLock()
		for k, v := range db.shards[i].entries {
			if !v.IsExpired(now) {
				result[k] = v
			}
		}
		db.shards[i].mu.RUnlock()
	}
	return result
}

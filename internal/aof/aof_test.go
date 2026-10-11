package aof

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Jigar-23/flashdb/internal/engine"
)

func TestAOFWriteAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	aofPath := filepath.Join(tmpDir, "test.aof")

	// 1. Open AOF and write commands
	a, err := Open(aofPath, FsyncAlways)
	if err != nil {
		t.Fatalf("failed to open AOF: %v", err)
	}

	_ = a.WriteCommand("SET", "hero", "batman")
	_ = a.WriteCommand("RPUSH", "heroes", "batman", "superman", "flash")
	_ = a.WriteCommand("HSET", "profile:1", "name", "Bruce")
	_ = a.WriteCommand("SADD", "skills", "detective", "martial_arts")
	_ = a.Close()

	// 2. Open fresh DB and replay
	db := engine.NewDB(engine.Config{})
	defer db.Close()

	a2, err := Open(aofPath, FsyncAlways)
	if err != nil {
		t.Fatalf("failed to re-open AOF: %v", err)
	}
	defer a2.Close()

	if err := a2.Replay(db); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// 3. Verify restored data
	val, ok, err := db.Get("hero")
	if err != nil || !ok || val != "batman" {
		t.Fatalf("Get hero failed: val=%v, ok=%v", val, ok)
	}

	list, err := db.LRange("heroes", 0, -1)
	if err != nil || len(list) != 3 || list[1] != "superman" {
		t.Fatalf("LRange heroes failed: list=%v", list)
	}

	hval, ok, err := db.HGet("profile:1", "name")
	if err != nil || !ok || hval != "Bruce" {
		t.Fatalf("HGet failed: val=%v, ok=%v", hval, ok)
	}

	isMem, err := db.SIsMember("skills", "detective")
	if err != nil || !isMem {
		t.Fatalf("SIsMember failed: isMem=%v", isMem)
	}
}

func TestAOFRewriteCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	aofPath := filepath.Join(tmpDir, "rewrite.aof")

	db := engine.NewDB(engine.Config{})
	defer db.Close()

	a, err := Open(aofPath, FsyncAlways)
	if err != nil {
		t.Fatalf("failed to open AOF: %v", err)
	}
	defer a.Close()

	// Simulate thousands of repetitive writes
	_ = db.Set("counter", "1", 0)
	_ = a.WriteCommand("SET", "counter", "1")
	_ = db.Set("counter", "2", 0)
	_ = a.WriteCommand("SET", "counter", "2")
	_ = db.Set("counter", "100", 0)
	_ = a.WriteCommand("SET", "counter", "100")

	statBefore, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}

	// Trigger Rewrite
	if err := a.Rewrite(db); err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	statAfter, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat after failed: %v", err)
	}

	if statAfter.Size() >= statBefore.Size() {
		t.Errorf("compacted AOF size (%d) should be smaller than original (%d)", statAfter.Size(), statBefore.Size())
	}

	// Verify new DB can load from compacted AOF
	freshDB := engine.NewDB(engine.Config{})
	defer freshDB.Close()

	if err := a.Replay(freshDB); err != nil {
		t.Fatalf("replay of compacted AOF failed: %v", err)
	}

	val, ok, _ := freshDB.Get("counter")
	if !ok || val != "100" {
		t.Fatalf("expected counter 100, got %v", val)
	}
}

package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Jigar-23/flashdb/internal/engine"
	"github.com/Jigar-23/flashdb/internal/resp"
)

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

type AOF struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filename string
	policy   FsyncPolicy
	stopChan chan struct{}
}

// Open creates or opens an AOF file
func Open(filename string, policy FsyncPolicy) (*AOF, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	a := &AOF{
		file:      file,
		writer:    bufio.NewWriter(file),
		filename:  filename,
		policy:    policy,
		stopChan: make(chan struct{}),
	}

	if policy == FsyncEverySec {
		go a.everySecFsyncLoop()
	}

	return a, nil
}

// WriteCommand serializes a command into RESP array and appends to AOF
func (a *AOF) WriteCommand(args ...string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.MakeBulkString(arg)
	}

	w := resp.NewWriter(a.writer)
	if err := w.WriteArray(values); err != nil {
		return err
	}
	if err := a.writer.Flush(); err != nil {
		return err
	}

	if a.policy == FsyncAlways {
		return a.file.Sync()
	}
	return nil
}

func (a *AOF) everySecFsyncLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopChan:
			return
		case <-ticker.C:
			a.mu.Lock()
			_ = a.writer.Flush()
			_ = a.file.Sync()
			a.mu.Unlock()
		}
	}
}

// Close flushes and closes the AOF
func (a *AOF) Close() error {
	close(a.stopChan)
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.writer.Flush(); err != nil {
		return err
	}
	if err := a.file.Sync(); err != nil {
		return err
	}
	return a.file.Close()
}

// Replay reads the AOF file and re-executes all mutating commands into the DB
func (a *AOF) Replay(db *engine.DB) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Seek(0, 0); err != nil {
		return err
	}

	reader := resp.NewReader(a.file)
	for {
		cmd, err := reader.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("corrupted AOF log: %w", err)
		}
		if len(cmd) == 0 {
			continue
		}

		op := cmd[0]
		switch op {
		case "SET":
			if len(cmd) >= 3 {
				ttl := int64(0)
				if len(cmd) >= 5 && (cmd[3] == "EX" || cmd[3] == "ex") {
					secs, _ := strconv.ParseInt(cmd[4], 10, 64)
					ttl = secs * 1000
				}
				_ = db.Set(cmd[1], cmd[2], ttl)
			}
		case "DEL":
			for i := 1; i < len(cmd); i++ {
				db.Del(cmd[i])
			}
		case "EXPIRE":
			if len(cmd) == 3 {
				secs, _ := strconv.ParseInt(cmd[2], 10, 64)
				db.Expire(cmd[1], secs*1000)
			}
		case "LPUSH":
			if len(cmd) >= 3 {
				_, _ = db.LPush(cmd[1], cmd[2:]...)
			}
		case "RPUSH":
			if len(cmd) >= 3 {
				_, _ = db.RPush(cmd[1], cmd[2:]...)
			}
		case "LPOP":
			if len(cmd) == 2 {
				_, _, _ = db.LPop(cmd[1])
			}
		case "RPOP":
			if len(cmd) == 2 {
				_, _, _ = db.RPop(cmd[1])
			}
		case "HSET":
			if len(cmd) == 4 {
				_, _ = db.HSet(cmd[1], cmd[2], cmd[3])
			}
		case "HDEL":
			if len(cmd) >= 3 {
				_, _ = db.HDel(cmd[1], cmd[2:]...)
			}
		case "SADD":
			if len(cmd) >= 3 {
				_, _ = db.SAdd(cmd[1], cmd[2:]...)
			}
		case "SREM":
			if len(cmd) >= 3 {
				_, _ = db.SRem(cmd[1], cmd[2:]...)
			}
		case "FLUSHDB":
			db.FlushDB()
		}
	}

	// Seek to end of file for subsequent appends
	_, err := a.file.Seek(0, io.SeekEnd)
	return err
}

// Rewrite / Compacts the AOF file from current DB state
func (a *AOF) Rewrite(db *engine.DB) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	tempFilename := a.filename + ".rewrite.tmp"
	tempFile, err := os.OpenFile(tempFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(tempFilename)

	tempBuf := bufio.NewWriter(tempFile)
	w := resp.NewWriter(tempBuf)

	snapshot := db.Snapshot()
	now := time.Now().UnixMilli()

	for k, e := range snapshot {
		switch e.Type {
		case engine.TypeString:
			args := []resp.Value{resp.MakeBulkString("SET"), resp.MakeBulkString(k), resp.MakeBulkString(e.Value.(string))}
			if e.ExpiresAt > 0 {
				remSecs := (e.ExpiresAt - now) / 1000
				if remSecs > 0 {
					args = append(args, resp.MakeBulkString("EX"), resp.MakeBulkString(strconv.FormatInt(remSecs, 10)))
				}
			}
			if err := w.WriteArray(args); err != nil {
				tempFile.Close()
				return err
			}
		case engine.TypeList:
			list := e.Value.([]string)
			if len(list) > 0 {
				args := []resp.Value{resp.MakeBulkString("RPUSH"), resp.MakeBulkString(k)}
				for _, item := range list {
					args = append(args, resp.MakeBulkString(item))
				}
				if err := w.WriteArray(args); err != nil {
					tempFile.Close()
					return err
				}
			}
		case engine.TypeHash:
			hmap := e.Value.(map[string]string)
			for field, val := range hmap {
				args := []resp.Value{resp.MakeBulkString("HSET"), resp.MakeBulkString(k), resp.MakeBulkString(field), resp.MakeBulkString(val)}
				if err := w.WriteArray(args); err != nil {
					tempFile.Close()
					return err
				}
			}
		case engine.TypeSet:
			set := e.Value.(map[string]struct{})
			if len(set) > 0 {
				args := []resp.Value{resp.MakeBulkString("SADD"), resp.MakeBulkString(k)}
				for m := range set {
					args = append(args, resp.MakeBulkString(m))
				}
				if err := w.WriteArray(args); err != nil {
					tempFile.Close()
					return err
				}
			}
		}
	}

	if err := tempBuf.Flush(); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return err
	}
	tempFile.Close()

	// Atomically replace old AOF with compacted version
	_ = a.file.Close()
	if err := os.Rename(tempFilename, a.filename); err != nil {
		return err
	}

	// Reopen file
	newFile, err := os.OpenFile(a.filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	a.file = newFile
	a.writer = bufio.NewWriter(newFile)
	return nil
}

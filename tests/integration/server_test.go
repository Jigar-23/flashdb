package integration

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jigar-23/flashdb/internal/resp"
	"github.com/Jigar-23/flashdb/internal/server"
)

func TestServerEndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	aofPath := filepath.Join(tmpDir, "e2e.aof")

	srv, err := server.NewServer(server.Config{
		Host:      "127.0.0.1",
		Port:      16379,
		AOFPath:   aofPath,
		AOFPolicy: "always",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		_ = srv.Start()
	}()
	defer srv.Close()

	// Wait for server to listen
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)

	sendCmd := func(args ...string) resp.Value {
		vals := make([]resp.Value, len(args))
		for i, a := range args {
			vals[i] = resp.MakeBulkString(a)
		}
		if err := w.WriteArray(vals); err != nil {
			t.Fatalf("sendCmd write error: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("sendCmd flush error: %v", err)
		}
		reply, err := r.ReadValue()
		if err != nil {
			t.Fatalf("sendCmd read error: %v", err)
		}
		return reply
	}

	// 1. PING
	reply := sendCmd("PING")
	if reply.Str != "PONG" {
		t.Errorf("PING expected PONG, got %v", reply.Str)
	}

	// 2. SET and GET
	reply = sendCmd("SET", "user:id", "9999")
	if reply.Str != "OK" {
		t.Errorf("SET expected OK, got %v", reply.Str)
	}

	reply = sendCmd("GET", "user:id")
	if string(reply.Bulk) != "9999" {
		t.Errorf("GET user:id expected 9999, got %v", string(reply.Bulk))
	}

	// 3. INCR
	reply = sendCmd("INCR", "user:id")
	if reply.Num != 10000 {
		t.Errorf("INCR expected 10000, got %v", reply.Num)
	}

	// 4. LPUSH and LRANGE
	reply = sendCmd("RPUSH", "tasks", "task1", "task2", "task3")
	if reply.Num != 3 {
		t.Errorf("RPUSH expected 3, got %v", reply.Num)
	}
	reply = sendCmd("LRANGE", "tasks", "0", "-1")
	if len(reply.Array) != 3 {
		t.Errorf("LRANGE expected 3 items, got %d", len(reply.Array))
	}

	// 5. HSET and HGETALL
	reply = sendCmd("HSET", "meta", "author", "Jigar")
	if reply.Num != 1 {
		t.Errorf("HSET expected 1, got %v", reply.Num)
	}
	reply = sendCmd("HGETALL", "meta")
	if len(reply.Array) != 2 {
		t.Errorf("HGETALL expected 2 values (k, v), got %d", len(reply.Array))
	}

	// 6. SADD and SISMEMBER
	reply = sendCmd("SADD", "tags", "golang", "systems")
	if reply.Num != 2 {
		t.Errorf("SADD expected 2, got %v", reply.Num)
	}
	reply = sendCmd("SISMEMBER", "tags", "golang")
	if reply.Num != 1 {
		t.Errorf("SISMEMBER expected 1, got %v", reply.Num)
	}

	// 7. DBSIZE
	reply = sendCmd("DBSIZE")
	if reply.Num < 4 {
		t.Errorf("DBSIZE expected >= 4, got %v", reply.Num)
	}

	// 8. INFO
	reply = sendCmd("INFO")
	if len(reply.Bulk) == 0 {
		t.Errorf("INFO returned empty string")
	}
}

func TestConcurrentClients(t *testing.T) {
	tmpDir := t.TempDir()
	aofPath := filepath.Join(tmpDir, "concurrent.aof")

	srv, err := server.NewServer(server.Config{
		Host:      "127.0.0.1",
		Port:      16380,
		AOFPath:   aofPath,
		AOFPolicy: "no",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		_ = srv.Start()
	}()
	defer srv.Close()

	time.Sleep(100 * time.Millisecond)

	const clients = 20
	errChan := make(chan error, clients)

	for c := 0; c < clients; c++ {
		clientID := c
		go func() {
			conn, err := net.Dial("tcp", "127.0.0.1:16380")
			if err != nil {
				errChan <- err
				return
			}
			defer conn.Close()

			r := resp.NewReader(conn)
			w := resp.NewWriter(conn)

			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("k:%d:%d", clientID, i)
				val := fmt.Sprintf("v:%d", i)

				// SET
				_ = w.WriteArray([]resp.Value{
					resp.MakeBulkString("SET"),
					resp.MakeBulkString(key),
					resp.MakeBulkString(val),
				})
				_ = w.Flush()
				res, err := r.ReadValue()
				if err != nil || res.Str != "OK" {
					errChan <- fmt.Errorf("SET failed: %v, res=%+v", err, res)
					return
				}

				// GET
				_ = w.WriteArray([]resp.Value{
					resp.MakeBulkString("GET"),
					resp.MakeBulkString(key),
				})
				_ = w.Flush()
				res, err = r.ReadValue()
				if err != nil || string(res.Bulk) != val {
					errChan <- fmt.Errorf("GET mismatch for %s: got=%s, want=%s", key, string(res.Bulk), val)
					return
				}
			}
			errChan <- nil
		}()
	}

	for c := 0; c < clients; c++ {
		if err := <-errChan; err != nil {
			t.Fatalf("concurrent client error: %v", err)
		}
	}
}

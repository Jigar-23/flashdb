package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Jigar-23/flashdb/internal/aof"
	"github.com/Jigar-23/flashdb/internal/engine"
	"github.com/Jigar-23/flashdb/internal/resp"
)

type Config struct {
	Host           string
	Port           int
	AOFPath        string
	AOFPolicy      string
	MaxMemory      int64
	EvictionPolicy string
}

type Server struct {
	cfg            Config
	db             *engine.DB
	aof            *aof.AOF
	listener       net.Listener
	mu             sync.Mutex
	conns          map[net.Conn]struct{}
	stopChan       chan struct{}
	running        int32
	startTime      time.Time
	totalOps       int64
	totalConns     int64
	activeConns    int64
}

// NewServer initializes FlashDB Server
func NewServer(cfg Config) (*Server, error) {
	if cfg.Port <= 0 {
		cfg.Port = 6379
	}
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}

	dbCfg := engine.Config{
		MaxMemory:      cfg.MaxMemory,
		EvictionPolicy: cfg.EvictionPolicy,
	}
	db := engine.NewDB(dbCfg)

	var aofEngine *aof.AOF
	if cfg.AOFPath != "" {
		policy := aof.FsyncEverySec
		if cfg.AOFPolicy == "always" {
			policy = aof.FsyncAlways
		} else if cfg.AOFPolicy == "no" {
			policy = aof.FsyncNo
		}

		var err error
		aofEngine, err = aof.Open(cfg.AOFPath, policy)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to open AOF file: %w", err)
		}

		// Replay existing AOF data
		if err := aofEngine.Replay(db); err != nil {
			db.Close()
			_ = aofEngine.Close()
			return nil, fmt.Errorf("failed to replay AOF log: %w", err)
		}
	}

	return &Server{
		cfg:       cfg,
		db:        db,
		aof:       aofEngine,
		conns:     make(map[net.Conn]struct{}),
		stopChan:  make(chan struct{}),
		startTime: time.Now(),
	}, nil
}

// Start runs the TCP socket server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener
	atomic.StoreInt32(&s.running, 1)

	// Catch OS signals for graceful shutdown
	go s.handleSignals()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return nil
			default:
				return err
			}
		}

		atomic.AddInt64(&s.totalConns, 1)
		atomic.AddInt64(&s.activeConns, 1)

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go s.handleConnection(conn)
	}
}

func (s *Server) handleSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		_ = s.Close()
	case <-s.stopChan:
	}
}

// Close stops listener, flushes AOF, and closes connections
func (s *Server) Close() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}
	close(s.stopChan)

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.mu.Lock()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	if s.aof != nil {
		_ = s.aof.Close()
	}
	s.db.Close()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		atomic.AddInt64(&s.activeConns, -1)
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		cmd, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			// Write syntax error and close
			_ = writer.WriteError(fmt.Sprintf("ERR protocol error: %v", err))
			_ = writer.Flush()
			return
		}

		if len(cmd) == 0 {
			continue
		}

		atomic.AddInt64(&s.totalOps, 1)
		reply := s.executeCommand(cmd)

		if err := writer.WriteValue(reply); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) executeCommand(args []string) resp.Value {
	name := strings.ToUpper(args[0])

	switch name {
	// Connection / Server
	case "PING":
		if len(args) == 1 {
			return resp.MakeSimpleString("PONG")
		}
		return resp.MakeBulkString(args[1])

	case "ECHO":
		if len(args) < 2 {
			return resp.MakeError("ERR wrong number of arguments for 'echo' command")
		}
		return resp.MakeBulkString(args[1])

	case "COMMAND":
		// Required for redis-cli compatibility
		return resp.MakeArray([]resp.Value{})

	case "INFO":
		return resp.MakeBulkString(s.generateInfo())

	case "DBSIZE":
		return resp.MakeInteger(s.db.KeysCount())

	case "FLUSHDB":
		s.db.FlushDB()
		if s.aof != nil {
			_ = s.aof.WriteCommand("FLUSHDB")
		}
		return resp.MakeSimpleString("OK")

	case "REWRITEAOF":
		if s.aof == nil {
			return resp.MakeError("ERR AOF is not enabled")
		}
		go func() {
			_ = s.aof.Rewrite(s.db)
		}()
		return resp.MakeSimpleString("Background append only file rewriting started")

	// Key Management
	case "DEL":
		if len(args) < 2 {
			return resp.MakeError("ERR wrong number of arguments for 'del' command")
		}
		count := int64(0)
		for _, k := range args[1:] {
			if s.db.Del(k) {
				count++
			}
		}
		if s.aof != nil && count > 0 {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(count)

	case "EXISTS":
		if len(args) < 2 {
			return resp.MakeError("ERR wrong number of arguments for 'exists' command")
		}
		count := int64(0)
		for _, k := range args[1:] {
			if s.db.Exists(k) {
				count++
			}
		}
		return resp.MakeInteger(count)

	case "EXPIRE":
		if len(args) != 3 {
			return resp.MakeError("ERR wrong number of arguments for 'expire' command")
		}
		secs, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return resp.MakeError("ERR value is not an integer or out of range")
		}
		ok := s.db.Expire(args[1], secs*1000)
		if ok {
			if s.aof != nil {
				_ = s.aof.WriteCommand(args...)
			}
			return resp.MakeInteger(1)
		}
		return resp.MakeInteger(0)

	case "PEXPIRE":
		if len(args) != 3 {
			return resp.MakeError("ERR wrong number of arguments for 'pexpire' command")
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return resp.MakeError("ERR value is not an integer or out of range")
		}
		if s.db.Expire(args[1], ms) {
			return resp.MakeInteger(1)
		}
		return resp.MakeInteger(0)

	case "TTL":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'ttl' command")
		}
		return resp.MakeInteger(s.db.TTL(args[1]))

	case "PTTL":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'pttl' command")
		}
		return resp.MakeInteger(s.db.PTTL(args[1]))

	case "PERSIST":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'persist' command")
		}
		if s.db.Persist(args[1]) {
			return resp.MakeInteger(1)
		}
		return resp.MakeInteger(0)

	// String Commands
	case "SET":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'set' command")
		}
		key, val := args[1], args[2]
		ttlMs := int64(0)

		if len(args) >= 5 {
			opt := strings.ToUpper(args[3])
			if opt == "EX" {
				secs, err := strconv.ParseInt(args[4], 10, 64)
				if err != nil {
					return resp.MakeError("ERR value is not an integer or out of range")
				}
				ttlMs = secs * 1000
			} else if opt == "PX" {
				ms, err := strconv.ParseInt(args[4], 10, 64)
				if err != nil {
					return resp.MakeError("ERR value is not an integer or out of range")
				}
				ttlMs = ms
			}
		}

		if err := s.db.Set(key, val, ttlMs); err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeSimpleString("OK")

	case "GET":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'get' command")
		}
		val, ok, err := s.db.Get(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if !ok {
			return resp.MakeNull()
		}
		return resp.MakeBulkString(val)

	case "MGET":
		if len(args) < 2 {
			return resp.MakeError("ERR wrong number of arguments for 'mget' command")
		}
		replies := make([]resp.Value, len(args)-1)
		for i, k := range args[1:] {
			val, ok, _ := s.db.Get(k)
			if !ok {
				replies[i] = resp.MakeNull()
			} else {
				replies[i] = resp.MakeBulkString(val)
			}
		}
		return resp.MakeArray(replies)

	case "MSET":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			return resp.MakeError("ERR wrong number of arguments for 'mset' command")
		}
		for i := 1; i < len(args); i += 2 {
			_ = s.db.Set(args[i], args[i+1], 0)
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeSimpleString("OK")

	case "INCR":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'incr' command")
		}
		newVal, err := s.db.Incr(args[1], 1)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand("SET", args[1], strconv.FormatInt(newVal, 10))
		}
		return resp.MakeInteger(newVal)

	case "DECR":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'decr' command")
		}
		newVal, err := s.db.Incr(args[1], -1)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand("SET", args[1], strconv.FormatInt(newVal, 10))
		}
		return resp.MakeInteger(newVal)

	case "APPEND":
		if len(args) != 3 {
			return resp.MakeError("ERR wrong number of arguments for 'append' command")
		}
		val, ok, err := s.db.Get(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		newVal := val + args[2]
		if !ok {
			newVal = args[2]
		}
		_ = s.db.Set(args[1], newVal, 0)
		if s.aof != nil {
			_ = s.aof.WriteCommand("SET", args[1], newVal)
		}
		return resp.MakeInteger(int64(len(newVal)))

	case "STRLEN":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'strlen' command")
		}
		val, ok, _ := s.db.Get(args[1])
		if !ok {
			return resp.MakeInteger(0)
		}
		return resp.MakeInteger(int64(len(val)))

	// List Commands
	case "LPUSH":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'lpush' command")
		}
		count, err := s.db.LPush(args[1], args[2:]...)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(count))

	case "RPUSH":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'rpush' command")
		}
		count, err := s.db.RPush(args[1], args[2:]...)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(count))

	case "LPOP":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'lpop' command")
		}
		val, ok, err := s.db.LPop(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if !ok {
			return resp.MakeNull()
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeBulkString(val)

	case "RPOP":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'rpop' command")
		}
		val, ok, err := s.db.RPop(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if !ok {
			return resp.MakeNull()
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeBulkString(val)

	case "LRANGE":
		if len(args) != 4 {
			return resp.MakeError("ERR wrong number of arguments for 'lrange' command")
		}
		start, err1 := strconv.Atoi(args[2])
		stop, err2 := strconv.Atoi(args[3])
		if err1 != nil || err2 != nil {
			return resp.MakeError("ERR value is not an integer or out of range")
		}
		items, err := s.db.LRange(args[1], start, stop)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		replies := make([]resp.Value, len(items))
		for i, it := range items {
			replies[i] = resp.MakeBulkString(it)
		}
		return resp.MakeArray(replies)

	// Hash Commands
	case "HSET":
		if len(args) != 4 {
			return resp.MakeError("ERR wrong number of arguments for 'hset' command")
		}
		res, err := s.db.HSet(args[1], args[2], args[3])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(res))

	case "HGET":
		if len(args) != 3 {
			return resp.MakeError("ERR wrong number of arguments for 'hget' command")
		}
		val, ok, err := s.db.HGet(args[1], args[2])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if !ok {
			return resp.MakeNull()
		}
		return resp.MakeBulkString(val)

	case "HDEL":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'hdel' command")
		}
		count, err := s.db.HDel(args[1], args[2:]...)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(count))

	case "HGETALL":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'hgetall' command")
		}
		hmap, err := s.db.HGetAll(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		replies := make([]resp.Value, 0, len(hmap)*2)
		for k, v := range hmap {
			replies = append(replies, resp.MakeBulkString(k), resp.MakeBulkString(v))
		}
		return resp.MakeArray(replies)

	// Set Commands
	case "SADD":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'sadd' command")
		}
		count, err := s.db.SAdd(args[1], args[2:]...)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(count))

	case "SMEMBERS":
		if len(args) != 2 {
			return resp.MakeError("ERR wrong number of arguments for 'smembers' command")
		}
		members, err := s.db.SMembers(args[1])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		replies := make([]resp.Value, len(members))
		for i, m := range members {
			replies[i] = resp.MakeBulkString(m)
		}
		return resp.MakeArray(replies)

	case "SREM":
		if len(args) < 3 {
			return resp.MakeError("ERR wrong number of arguments for 'srem' command")
		}
		count, err := s.db.SRem(args[1], args[2:]...)
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if s.aof != nil {
			_ = s.aof.WriteCommand(args...)
		}
		return resp.MakeInteger(int64(count))

	case "SISMEMBER":
		if len(args) != 3 {
			return resp.MakeError("ERR wrong number of arguments for 'sismember' command")
		}
		isMem, err := s.db.SIsMember(args[1], args[2])
		if err != nil {
			return resp.MakeError(err.Error())
		}
		if isMem {
			return resp.MakeInteger(1)
		}
		return resp.MakeInteger(0)

	default:
		return resp.MakeError(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
}

func (s *Server) generateInfo() string {
	uptime := int64(time.Since(s.startTime).Seconds())
	return fmt.Sprintf("# Server\r\n"+
		"flashdb_version:1.0.0\r\n"+
		"os:%s\r\n"+
		"uptime_in_seconds:%d\r\n"+
		"# Clients\r\n"+
		"connected_clients:%d\r\n"+
		"total_connections_received:%d\r\n"+
		"# Stats\r\n"+
		"total_commands_processed:%d\r\n"+
		"used_memory:%d\r\n"+
		"total_keys:%d\r\n",
		"darwin-arm64",
		uptime,
		atomic.LoadInt64(&s.activeConns),
		atomic.LoadInt64(&s.totalConns),
		atomic.LoadInt64(&s.totalOps),
		s.db.UsedMemory(),
		s.db.KeysCount(),
	)
}

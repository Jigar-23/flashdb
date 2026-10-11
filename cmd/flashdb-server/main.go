package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Jigar-23/flashdb/internal/server"
)

const Banner = `
  ______ _           _     _____  ____  
 |  ____| |         | |   |  __ \|  _ \ 
 | |__  | | __ _ ___| |__ | |  | | |_) |
 |  __| | |/ _` + "`" + ` / __| '_ \| |  | |  _ < 
 | |    | | (_| \__ \ | | | |__| | |_) |
 |_|    |_|\__,_|___/_| |_|_____/|____/ 
 High-Performance In-Memory Key-Value Store
 Protocol: RESP v2 | Sharded Concurrency | AOF Persistence
`

func main() {
	port := flag.Int("port", 6379, "TCP port to listen on")
	host := flag.String("host", "0.0.0.0", "Host/interface to bind to")
	aofPath := flag.String("aof", "appendonly.aof", "Path to Append-Only File (empty to disable)")
	aofPolicy := flag.String("aof-policy", "everysec", "Fsync policy: always, everysec, no")
	maxMemory := flag.Int64("maxmemory", 0, "Max memory limit in bytes (0 for unlimited)")
	eviction := flag.String("maxmemory-policy", "allkeys-lru", "Eviction policy")

	flag.Parse()

	fmt.Println(Banner)
	log.Printf("[FlashDB] Initializing on %s:%d (AOF: %s, Policy: %s)\n", *host, *port, *aofPath, *aofPolicy)

	srv, err := server.NewServer(server.Config{
		Host:           *host,
		Port:           *port,
		AOFPath:        *aofPath,
		AOFPolicy:      *aofPolicy,
		MaxMemory:      *maxMemory,
		EvictionPolicy: *eviction,
	})
	if err != nil {
		log.Fatalf("[FlashDB] Fatal startup error: %v\n", err)
	}

	log.Printf("[FlashDB] Server ready to accept client connections.\n")
	if err := srv.Start(); err != nil {
		log.Printf("[FlashDB] Server exited: %v\n", err)
	}
	os.Exit(0)
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jigar-23/flashdb/internal/resp"
)

func main() {
	host := flag.String("h", "127.0.0.1", "Server hostname")
	port := flag.Int("p", 6379, "Server port")
	benchMode := flag.Bool("bench", false, "Run high-throughput performance benchmark")
	clients := flag.Int("c", 50, "Number of concurrent benchmark clients")
	totalOps := flag.Int("n", 50000, "Total number of operations to benchmark")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)

	if *benchMode {
		runBenchmark(addr, *clients, *totalOps)
		return
	}

	runREPL(addr)
}

func runREPL(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Could not connect to FlashDB at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to FlashDB at %s\nType 'exit' or 'quit' to close.\n\n", addr)
	scanner := bufio.NewScanner(os.Stdin)
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		fmt.Printf("%s> ", addr)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}

		parts := strings.Fields(line)
		values := make([]resp.Value, len(parts))
		for i, p := range parts {
			values[i] = resp.MakeBulkString(p)
		}

		if err := writer.WriteArray(values); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			break
		}
		if err := writer.Flush(); err != nil {
			fmt.Printf("Error flushing: %v\n", err)
			break
		}

		reply, err := reader.ReadValue()
		if err != nil {
			fmt.Printf("Error reading reply: %v\n", err)
			break
		}

		printReply(reply)
	}
}

func printReply(v resp.Value) {
	if v.Null {
		fmt.Println("(nil)")
		return
	}
	switch v.Typ {
	case resp.TypeSimpleString:
		fmt.Printf("%s\n", v.Str)
	case resp.TypeError:
		fmt.Printf("(error) %s\n", v.Str)
	case resp.TypeInteger:
		fmt.Printf("(integer) %d\n", v.Num)
	case resp.TypeBulkString:
		fmt.Printf("\"%s\"\n", string(v.Bulk))
	case resp.TypeArray:
		if len(v.Array) == 0 {
			fmt.Println("(empty array)")
			return
		}
		for i, item := range v.Array {
			fmt.Printf("%d) ", i+1)
			printReply(item)
		}
	}
}

func runBenchmark(addr string, clients, totalOps int) {
	fmt.Printf("====================================================\n")
	fmt.Printf(" FlashDB Benchmark Runner: %s\n", addr)
	fmt.Printf(" Concurrency: %d clients | Operations: %d\n", clients, totalOps)
	fmt.Printf("====================================================\n\n")

	opsPerClient := totalOps / clients
	var wg sync.WaitGroup
	wg.Add(clients)

	latencies := make([]time.Duration, 0, totalOps)
	var latMu sync.Mutex

	start := time.Now()

	for c := 0; c < clients; c++ {
		clientID := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()

			reader := resp.NewReader(conn)
			writer := resp.NewWriter(conn)
			localLat := make([]time.Duration, 0, opsPerClient)

			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("bench:client:%d:key:%d", clientID, i)
				val := "benchmark_payload_val"

				opStart := time.Now()
				// SET command
				_ = writer.WriteArray([]resp.Value{
					resp.MakeBulkString("SET"),
					resp.MakeBulkString(key),
					resp.MakeBulkString(val),
				})
				_ = writer.Flush()
				_, _ = reader.ReadValue()
				localLat = append(localLat, time.Since(opStart))
			}

			latMu.Lock()
			latencies = append(latencies, localLat...)
			latMu.Unlock()
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	totalFinished := len(latencies)
	if totalFinished == 0 {
		fmt.Println("Benchmark failed to record completions.")
		return
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	opsPerSec := float64(totalFinished) / duration.Seconds()
	p50 := latencies[int(float64(totalFinished)*0.50)]
	p95 := latencies[int(float64(totalFinished)*0.95)]
	p99 := latencies[int(float64(totalFinished)*0.99)]
	pMax := latencies[totalFinished-1]

	fmt.Printf("Benchmark Results:\n")
	fmt.Printf("  • Total Ops:     %d\n", totalFinished)
	fmt.Printf("  • Duration:      %.2fs\n", duration.Seconds())
	fmt.Printf("  • Throughput:    %.0f ops/sec\n", opsPerSec)
	fmt.Printf("  • Latency (p50): %v\n", p50)
	fmt.Printf("  • Latency (p95): %v\n", p95)
	fmt.Printf("  • Latency (p99): %v\n", p99)
	fmt.Printf("  • Max Latency:   %v\n", pMax)
	fmt.Printf("====================================================\n")
}

// mkbangfs initializes a new BangFS filesystem in the backend
package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"bangfs/fuse"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envPortOrDefault(key string, fallback uint) uint {
	if v := os.Getenv(key); v != "" {
		if p, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint(p)
		}
	}
	return fallback
}

func main() {
	host := flag.String("host", envOrDefault("BANGFS_HOST", ""), "Riak host (env: BANGFS_HOST)")
	port := flag.Uint("port", envPortOrDefault("BANGFS_PORT", 8087), "Riak port (env: BANGFS_PORT)")
	namespace := flag.String("namespace", envOrDefault("BANGFS_NAMESPACE", ""), "Filesystem namespace (env: BANGFS_NAMESPACE)")

	flag.Parse()

	// Validate required args
	if *host == "" || *namespace == "" {
		log.Println("Error: -host and -namespace are required (or set BANGFS_HOST, BANGFS_NAMESPACE)")
		flag.Usage()
		os.Exit(1)
	}

	// Connect to backend
	log.Printf("Connecting to Riak at %s:%d", *host, *port)
	kv, err := fuse.NewKVStore(*host, uint16(*port), *namespace)
	if err != nil {
		log.Fatalf("Failed to connect to backend: %v\n\n%s", err, kv.SetupInstructions())
	}
	defer kv.Close()

	// Initialize filesystem
	log.Printf("Initializing filesystem with namespace '%s'", *namespace)
	if err := kv.InitBackend(); err != nil {
		log.Fatalf("Failed to initialize filesystem: %v", err)
	}

	log.Printf("Filesystem initialized successfully!")
	log.Printf("  Metadata bucket: %s_bangfs_metadata", *namespace)
	log.Printf("  Chunk bucket:    %s_bangfs_chunks", *namespace)
	log.Printf("\nMount with: bangfs -host %s -port %d -namespace %s -mount /your/mountpoint", *host, *port, *namespace)
}

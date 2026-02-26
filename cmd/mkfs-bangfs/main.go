// mkbangfs initializes a new BangFS filesystem in the backend
package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"bangfs/bangfuse"
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
	host := flag.String("host", envOrDefault("RIAK_HOST", ""), "Riak host (env: RIAK_HOST)")
	port := flag.Uint("port", envPortOrDefault("RIAK_PORT", 8087), "Riak port (env: RIAK_PORT)")
	namespace := flag.String("namespace", envOrDefault("BANGFS_NAMESPACE", ""), "Filesystem namespace (env: BANGFS_NAMESPACE)")
	dummy := flag.Bool("dummy", false, "Use file-backed store under /tmp instead of Riak")

	flag.Parse()

	if *namespace == "" {
		log.Println("Error: -namespace is required (or set BANGFS_NAMESPACE)")
		flag.Usage()
		os.Exit(1)
	}

	var kv bangfuse.KVStore
	if *dummy {
		log.Printf("Using file-backed store (namespace=%s)", *namespace)
		fkv, err := bangfuse.NewFileKVStore(*namespace)
		if err != nil {
			log.Fatalf("Failed to create file store: %v", err)
		}
		kv = fkv
	} else {
		if *host == "" {
			log.Println("Error: -host is required (or set RIAK_HOST), or use -dummy")
			flag.Usage()
			os.Exit(1)
		}
		log.Printf("Connecting to Riak at %s:%d", *host, *port)
		rkv, err := bangfuse.NewRiakKVStore(*host, uint16(*port), *namespace)
		if err != nil {
			log.Fatalf("Failed to connect to backend: %v", err)
		}
		kv = rkv
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
	log.Printf("\nMount with: mount-fuse-bangfs -host %s -port %d -namespace %s -mount /your/mountpoint", *host, *port, *namespace)
}

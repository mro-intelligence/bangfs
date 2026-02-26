// rmbangfs destroys a BangFS filesystem in the backend
// WARNING: This permanently deletes all data!
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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
	force := flag.Bool("force", false, "Skip confirmation prompt")

	flag.Parse()

	if *namespace == "" {
		log.Println("Error: -namespace is required (or set BANGFS_NAMESPACE)")
		flag.Usage()
		os.Exit(1)
	}

	// Confirm destruction unless -force
	if !*force {
		fmt.Printf("WARNING: This will permanently delete all data in namespace '%s'!\n", *namespace)
		fmt.Printf("  Metadata bucket: %s_bangfs_metadata\n", *namespace)
		fmt.Printf("  Chunk bucket:    %s_bangfs_chunks\n", *namespace)
		fmt.Print("\nType the namespace name to confirm: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input != *namespace {
			log.Fatal("Confirmation failed. Aborting.")
		}
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

	// Wipe filesystem data
	log.Printf("Wiping filesystem with namespace '%s'...", *namespace)
	if err := kv.WipeBackend(); err != nil {
		log.Fatalf("Failed to wipe filesystem: %v", err)
	}

	log.Printf("Filesystem destroyed successfully.")
}

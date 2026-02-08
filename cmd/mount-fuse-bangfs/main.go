// bangfs mounts a BangFS filesystem
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"

	"bangfs/fuse"
	"bangfs/util"
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
	mountpoint := flag.String("mount", envOrDefault("BANGFS_MOUNTDIR", ""), "Mount point (env: BANGFS_MOUNTDIR)")
	daemon := flag.Bool("daemon", false, "Run in background (daemon mode)")
	daemonChild := flag.Bool("daemon-child", false, "Internal flag for daemon mode")
	trace := flag.Bool("trace", false, "Enable tracing output for debugging")

	flag.Parse()

	// Enable tracing if requested
	if *trace {
		util.GetTracer().Enable()
	}

	// Validate required args
	if *host == "" || *namespace == "" || *mountpoint == "" {
		log.Println("Error: -host, -namespace, and -mount are required (or set RIAK_HOST, BANGFS_NAMESPACE, BANGFS_MOUNTDIR)")
		flag.Usage()
		os.Exit(1)
	}

	// If daemon mode requested and not already the child, re-exec in background
	if *daemon && !*daemonChild {
		args := append(os.Args[1:], "-daemon-child")
		cmd := exec.Command(os.Args[0], args...)
		cmd.Stdout = nil
		cmd.Stderr = nil
		cmd.Stdin = nil
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			log.Fatalf("Failed to start daemon: %v", err)
		}
		log.Printf("BangFS daemon started (pid %d)", cmd.Process.Pid)
		os.Exit(0)
	}

	// Connect to backend
	log.Printf("Connecting to Riak at %s:%d", *host, *port)
	kv, err := fuse.NewKVStore(*host, uint16(*port), *namespace)
	if err != nil {
		log.Fatalf("Failed to connect to backend: %v\n\n%s", err, kv.SetupInstructions())
	}
	defer kv.Close()

	// Verify filesystem exists (inode 0)
	_, _, err = kv.Metadata(0)
	if err != nil {
		log.Fatalf("Filesystem not initialized. Run mkbangfs first.\n\n%s", kv.SetupInstructions())
	}

	log.Printf("Mounting BangFS (namespace=%s) at %s", *namespace, *mountpoint)
	server, err := fuse.Mount(*mountpoint, kv)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	// Handle Ctrl-C and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received %v, unmounting...", sig)
		if err := server.Unmount(); err != nil {
			log.Printf("Unmount error: %v", err)
		}
	}()

	// Wait for unmount (either from signal or external umount command)
	server.Wait()
	log.Println("Unmounted successfully")
}

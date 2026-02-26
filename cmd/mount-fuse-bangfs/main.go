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

	"bangfs/bangfuse"
	"bangfs/bangutil"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	host := flag.String("host", envOrDefault("RIAK_HOST", ""), "Riak host (env: RIAK_HOST)")
	portDefault := uint(8087)
	if v, err := strconv.ParseUint(envOrDefault("RIAK_PORT", "8087"), 10, 16); err == nil {
		portDefault = uint(v)
	}
	port := flag.Uint("port", portDefault, "Riak port (env: RIAK_PORT)")
	namespace := flag.String("namespace", envOrDefault("BANGFS_NAMESPACE", ""), "Filesystem namespace (env: BANGFS_NAMESPACE)")
	mountpoint := flag.String("mount", envOrDefault("BANGFS_MOUNTDIR", ""), "Mount point (env: BANGFS_MOUNTDIR)")
	dummy := flag.Bool("dummy", false, "Use file-backed store under /tmp instead of Riak")
	daemon := flag.Bool("daemon", false, "Run in background (daemon mode)")
	daemonChild := flag.Bool("daemon-child", false, "Internal flag for daemon mode")
	trace := flag.Bool("trace", false, "Enable tracing output for debugging")
	tracelog := flag.String("tracelog", "", "Write trace output to file instead of stderr")

	flag.Parse()

	if *trace {
		tracer := bangutil.GetTracer()
		if *tracelog != "" {
			if err := tracer.SetOutputFile(*tracelog); err != nil {
				log.Fatalf("Failed to open trace log: %v", err)
			}
			defer tracer.CloseOutput()
		}
		tracer.Enable()
	}

	// Validate required args
	if *namespace == "" || *mountpoint == "" {
		log.Println("Error: -namespace and -mount are required (or set BANGFS_NAMESPACE, BANGFS_MOUNTDIR)")
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

	var bs *bangfuse.BangServer
	if *dummy {
		log.Printf("Using file-backed store (namespace=%s)", *namespace)
		fkv, err := bangfuse.NewFileKVStore(*namespace)
		if err != nil {
			log.Fatalf("Failed to create file store: %v", err)
		}
		bs, err = bangfuse.NewBangServerWithKV(fkv)
		if err != nil {
			log.Fatalf("Failed to initialize: %v", err)
		}
	} else {
		if *host == "" {
			log.Println("Error: -host is required (or set RIAK_HOST), or use -dummy")
			flag.Usage()
			os.Exit(1)
		}
		log.Printf("Connecting to Riak at %s:%d", *host, *port)
		var err error
		bs, err = bangfuse.NewBangServer(*host, uint16(*port), *namespace)
		if err != nil {
			log.Fatalf("Failed to initialize: %v", err)
		}
	}
	defer bs.Close()

	log.Printf("Mounting BangFS (namespace=%s) at %s", *namespace, *mountpoint)
	if err := bs.Mount(*mountpoint); err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	// Handle Ctrl-C and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received %v, unmounting...", sig)
		if err := bs.Unmount(); err != nil {
			log.Printf("Unmount error: %v", err)
		}
	}()

	// Wait for unmount (either from signal or external umount command)
	bs.Wait()
	log.Println("Unmounted successfully")
}

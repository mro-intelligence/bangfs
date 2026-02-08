#!/usr/bin/env python3
"""
BangFS Filesystem Test Suite

Tests basic FUSE operations against a mounted BangFS filesystem.

Usage:
    python3 test_bangfs.py                    # Full setup/test/teardown
    python3 test_bangfs.py --no-setup         # Skip setup, just run tests
    python3 test_bangfs.py --no-teardown      # Skip teardown after tests
    python3 test_bangfs.py /path/to/mount     # Use custom mountpoint (legacy mode)

Configuration (via environment or defaults):
    RIAK_HOST=172.17.0.2
    RIAK_PORT=8087
    BANGFS_NAMESPACE=foobar
    BANGFS_MOUNTDIR=/tmp/bangfs

Expected: lots of RED initially, progressively turning GREEN as you implement.
"""

import argparse
import os
import signal
import sys
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
from enum import Enum

# Colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"

# Configuration defaults
DEFAULT_RIAK_HOST = "172.17.0.2"
DEFAULT_RIAK_PORT = "8087"
DEFAULT_NAMESPACE = "foobar"
DEFAULT_MOUNTPOINT = "/tmp/bangfs"


class Expected(Enum):
    SUCCESS = "success"          # command should succeed (exit 0)
    FAIL = "fail"                # command should fail (exit != 0)
    OUTPUT_CONTAINS = "contains" # stdout contains expected string
    OUTPUT_EQUALS = "equals"     # stdout equals expected string
    FILE_EXISTS = "exists"       # file should exist after command
    FILE_GONE = "gone"           # file should not exist after command
    CUSTOM = "custom"            # custom validation function


@dataclass
class Test:
    description: str
    command: str                           # shell command (use {mount}, {file}, etc.)
    expected: Expected
    expected_value: Optional[str] = None   # for OUTPUT_CONTAINS, OUTPUT_EQUALS
    setup: Optional[str] = None            # command to run before test
    cleanup: Optional[str] = None          # command to run after test
    check_path: Optional[str] = None       # for FILE_EXISTS, FILE_GONE


# ============================================================================
# TEST DEFINITIONS
# ============================================================================

TESTS = [
    # -------------------------------------------------------------------------
    # PHASE 1: Basic Read Operations (Getattr, Readdir)
    # -------------------------------------------------------------------------
    ("PHASE 1: Basic Read Operations (Getattr, Readdir)", [
        Test("stat root directory",
             "stat '{mount}'",
             Expected.SUCCESS),

        Test("root is a directory",
             "stat -c '%F' '{mount}'",
             Expected.OUTPUT_CONTAINS, "directory"),

        Test("ls root directory",
             "ls '{mount}'",
             Expected.SUCCESS),

        Test("ls -la root directory",
             "ls -la '{mount}'",
             Expected.SUCCESS),

        Test("root has permissions",
             "stat -c '%a' '{mount}'",
             Expected.SUCCESS),

        Test("stat non-existent file fails",
             "stat '{mount}/nonexistent'",
             Expected.FAIL),

        Test("ls non-existent dir fails",
             "ls '{mount}/nonexistent'",
             Expected.FAIL),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 2: Directory Operations (Mkdir, Rmdir, Lookup)
    # -------------------------------------------------------------------------
    ("PHASE 2: Directory Operations (Mkdir, Rmdir)", [
        Test("mkdir creates directory",
             "mkdir '{mount}/testdir'",
             Expected.SUCCESS),

        Test("directory exists after mkdir",
             "test -d '{mount}/testdir'",
             Expected.SUCCESS),

        Test("stat new directory",
             "stat '{mount}/testdir'",
             Expected.SUCCESS),

        Test("ls shows new directory",
             "ls '{mount}'",
             Expected.OUTPUT_CONTAINS, "testdir"),

        Test("mkdir nested directory",
             "mkdir '{mount}/testdir/nested'",
             Expected.SUCCESS),

        Test("rmdir nested directory",
             "rmdir '{mount}/testdir/nested'",
             Expected.SUCCESS),

        Test("nested dir is gone",
             "test -d '{mount}/testdir/nested'",
             Expected.FAIL),

        Test("rmdir testdir",
             "rmdir '{mount}/testdir'",
             Expected.SUCCESS),

        Test("testdir is gone",
             "test -d '{mount}/testdir'",
             Expected.FAIL),

        Test("mkdir -p creates nested path",
             "mkdir -p '{mount}/a/b/c'",
             Expected.SUCCESS),

        Test("nested path exists",
             "test -d '{mount}/a/b/c'",
             Expected.SUCCESS),

        Test("cleanup nested path",
             "rm -rf '{mount}/a'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 3: File Creation (Create, Unlink, Lookup)
    # -------------------------------------------------------------------------
    ("PHASE 3: File Creation (Create, Unlink)", [
        Test("touch creates empty file",
             "touch '{mount}/testfile.txt'",
             Expected.SUCCESS),

        Test("file exists after touch",
             "test -f '{mount}/testfile.txt'",
             Expected.SUCCESS),

        Test("stat file works",
             "stat '{mount}/testfile.txt'",
             Expected.SUCCESS),

        Test("file is regular file",
             "stat -c '%F' '{mount}/testfile.txt'",
             Expected.OUTPUT_CONTAINS, "regular"),

        Test("empty file has size 0",
             "stat -c '%s' '{mount}/testfile.txt'",
             Expected.OUTPUT_EQUALS, "0"),

        Test("ls shows file",
             "ls '{mount}'",
             Expected.OUTPUT_CONTAINS, "testfile.txt"),

        Test("rm removes file",
             "rm '{mount}/testfile.txt'",
             Expected.SUCCESS),

        Test("file is gone after rm",
             "test -f '{mount}/testfile.txt'",
             Expected.FAIL),

        Test("rm non-existent file fails",
             "rm '{mount}/nonexistent'",
             Expected.FAIL),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 4: File Write Operations
    # -------------------------------------------------------------------------
    ("PHASE 4: File Write Operations (Write)", [
        Test("echo writes to file",
             "echo 'Hello, BangFS!' > '{mount}/hello.txt'",
             Expected.SUCCESS),

        Test("file exists after write",
             "test -f '{mount}/hello.txt'",
             Expected.SUCCESS),

        Test("file has correct size",
             "stat -c '%s' '{mount}/hello.txt'",
             Expected.OUTPUT_EQUALS, "15"),  # "Hello, BangFS!\n"

        Test("cat reads file content",
             "cat '{mount}/hello.txt'",
             Expected.OUTPUT_CONTAINS, "Hello, BangFS!"),

        Test("echo append works",
             "echo 'Second line' >> '{mount}/hello.txt'",
             Expected.SUCCESS),

        Test("appended content is there",
             "cat '{mount}/hello.txt'",
             Expected.OUTPUT_CONTAINS, "Second line"),

        Test("file has both lines",
             "wc -l < '{mount}/hello.txt'",
             Expected.OUTPUT_EQUALS, "2"),

        Test("cleanup hello.txt",
             "rm '{mount}/hello.txt'",
             Expected.SUCCESS),

        Test("write binary data",
             "dd if=/dev/zero of='{mount}/zeros.bin' bs=1024 count=10 2>/dev/null",
             Expected.SUCCESS),

        Test("binary file has correct size",
             "stat -c '%s' '{mount}/zeros.bin'",
             Expected.OUTPUT_EQUALS, "10240"),

        Test("cleanup binary file",
             "rm '{mount}/zeros.bin'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 5: File Read Operations
    # -------------------------------------------------------------------------
    ("PHASE 5: File Read Operations (Read)", [
        Test("setup: create file with known content",
             "echo -n 'ABCDEFGHIJ' > '{mount}/readtest.txt'",
             Expected.SUCCESS),

        Test("cat reads entire file",
             "cat '{mount}/readtest.txt'",
             Expected.OUTPUT_EQUALS, "ABCDEFGHIJ"),

        Test("head reads first bytes",
             "head -c 5 '{mount}/readtest.txt'",
             Expected.OUTPUT_EQUALS, "ABCDE"),

        Test("tail reads last bytes",
             "tail -c 5 '{mount}/readtest.txt'",
             Expected.OUTPUT_EQUALS, "FGHIJ"),

        Test("dd reads with offset",
             "dd if='{mount}/readtest.txt' bs=1 skip=3 count=4 2>/dev/null",
             Expected.OUTPUT_EQUALS, "DEFG"),

        Test("cleanup readtest.txt",
             "rm '{mount}/readtest.txt'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 6: Large Files (multiple chunks)
    # -------------------------------------------------------------------------
    ("PHASE 6: Large Files (multiple chunks)", [
        Test("write 1MB file",
             "dd if=/dev/urandom of='{mount}/large.bin' bs=1M count=1 2>/dev/null",
             Expected.SUCCESS),

        Test("large file has correct size",
             "stat -c '%s' '{mount}/large.bin'",
             Expected.OUTPUT_EQUALS, "1048576"),

        Test("can compute md5 of large file",
             "md5sum '{mount}/large.bin'",
             Expected.SUCCESS),

        Test("cleanup large file",
             "rm '{mount}/large.bin'",
             Expected.SUCCESS),

        Test("write 5MB file",
             "dd if=/dev/urandom of='{mount}/bigger.bin' bs=1M count=5 2>/dev/null",
             Expected.SUCCESS),

        Test("5MB file has correct size",
             "stat -c '%s' '{mount}/bigger.bin'",
             Expected.OUTPUT_EQUALS, "5242880"),

        Test("cleanup 5MB file",
             "rm '{mount}/bigger.bin'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 7: Files in Subdirectories
    # -------------------------------------------------------------------------
    ("PHASE 7: Files in Subdirectories", [
        Test("create subdirectory",
             "mkdir '{mount}/subdir'",
             Expected.SUCCESS),

        Test("create file in subdirectory",
             "echo 'nested content' > '{mount}/subdir/nested.txt'",
             Expected.SUCCESS),

        Test("read file in subdirectory",
             "cat '{mount}/subdir/nested.txt'",
             Expected.OUTPUT_CONTAINS, "nested content"),

        Test("ls -R shows nested structure",
             "ls -R '{mount}'",
             Expected.OUTPUT_CONTAINS, "nested.txt"),

        Test("find locates nested file",
             "find '{mount}' -name 'nested.txt'",
             Expected.OUTPUT_CONTAINS, "nested.txt"),

        Test("cleanup nested file",
             "rm '{mount}/subdir/nested.txt'",
             Expected.SUCCESS),

        Test("cleanup subdirectory",
             "rmdir '{mount}/subdir'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 8: Edge Cases and Error Handling
    # -------------------------------------------------------------------------
    ("PHASE 8: Edge Cases and Error Handling", [
        Test("file with spaces in name",
             "touch '{mount}/file with spaces.txt'",
             Expected.SUCCESS),

        Test("can stat file with spaces",
             "stat '{mount}/file with spaces.txt'",
             Expected.SUCCESS),

        Test("cleanup file with spaces",
             "rm '{mount}/file with spaces.txt'",
             Expected.SUCCESS),

        Test("file with special chars",
             "touch '{mount}/file-with_special.chars.txt'",
             Expected.SUCCESS),

        Test("cleanup special chars file",
             "rm '{mount}/file-with_special.chars.txt'",
             Expected.SUCCESS),

        Test("rmdir on non-empty dir fails",
             "mkdir '{mount}/nonempty' && touch '{mount}/nonempty/file' && rmdir '{mount}/nonempty'",
             Expected.FAIL),

        Test("cleanup non-empty test",
             "rm -rf '{mount}/nonempty'",
             Expected.SUCCESS),

        Test("rm on directory fails",
             "mkdir '{mount}/rmtest' && rm '{mount}/rmtest'",
             Expected.FAIL),

        Test("cleanup rm test",
             "rmdir '{mount}/rmtest' 2>/dev/null; true",
             Expected.SUCCESS),

        Test("cat non-existent file fails",
             "cat '{mount}/does_not_exist'",
             Expected.FAIL),

        Test("cannot mkdir over existing file",
             "touch '{mount}/afile' && mkdir '{mount}/afile'",
             Expected.FAIL),

        Test("cleanup afile",
             "rm -f '{mount}/afile'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 9: Overwrite and Truncate
    # -------------------------------------------------------------------------
    ("PHASE 9: Overwrite and Truncate", [
        Test("create initial file",
             "echo 'original content here' > '{mount}/overwrite.txt'",
             Expected.SUCCESS),

        Test("overwrite with shorter content",
             "echo 'short' > '{mount}/overwrite.txt'",
             Expected.SUCCESS),

        Test("content is replaced not appended",
             "cat '{mount}/overwrite.txt'",
             Expected.OUTPUT_EQUALS, "short"),

        Test("truncate to zero",
             "truncate -s 0 '{mount}/overwrite.txt'",
             Expected.SUCCESS),

        Test("file is now empty",
             "stat -c '%s' '{mount}/overwrite.txt'",
             Expected.OUTPUT_EQUALS, "0"),

        Test("cleanup overwrite test",
             "rm '{mount}/overwrite.txt'",
             Expected.SUCCESS),
    ]),
]


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_command(cmd: str, timeout: int = 30) -> tuple[bool, str, str]:
    """Run a shell command, return (success, stdout, stderr)"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
    except subprocess.TimeoutExpired:
        return (False, "", "TIMEOUT")
    except Exception as e:
        return (False, "", str(e))


def run_test(test: Test, mount: str) -> bool:
    """Run a single test, return True if passed"""
    # Substitute {mount} in command
    cmd = test.command.format(mount=mount)

    # Run setup if present
    if test.setup:
        run_command(test.setup.format(mount=mount))

    # Run the test command
    success, stdout, stderr = run_command(cmd)

    # Evaluate result
    passed = False
    details = ""

    if test.expected == Expected.SUCCESS:
        passed = success
        if not passed:
            details = stderr or "command failed"

    elif test.expected == Expected.FAIL:
        passed = not success
        if not passed:
            details = "expected failure but succeeded"

    elif test.expected == Expected.OUTPUT_CONTAINS:
        passed = success and test.expected_value in stdout
        if not passed:
            details = f"expected '{test.expected_value}' in output, got: '{stdout}'"

    elif test.expected == Expected.OUTPUT_EQUALS:
        passed = success and stdout == test.expected_value
        if not passed:
            details = f"expected '{test.expected_value}', got: '{stdout}'"

    elif test.expected == Expected.FILE_EXISTS:
        path = test.check_path.format(mount=mount) if test.check_path else None
        passed = path and os.path.exists(path)
        if not passed:
            details = f"file {path} does not exist"

    elif test.expected == Expected.FILE_GONE:
        path = test.check_path.format(mount=mount) if test.check_path else None
        passed = path and not os.path.exists(path)
        if not passed:
            details = f"file {path} still exists"

    # Print result
    if passed:
        print(f"  {GREEN}PASS{RESET} {test.description}")
    else:
        print(f"  {RED}FAIL{RESET} {test.description}")
        if details:
            print(f"       {RED}{details}{RESET}")

    # Run cleanup if present
    if test.cleanup:
        run_command(test.cleanup.format(mount=mount))

    return passed


# ============================================================================
# SETUP / TEARDOWN
# ============================================================================

class BangFSSetup:
    """Handles setup and teardown of BangFS for testing"""

    def __init__(self, host: str, port: str, namespace: str, mountpoint: str):
        self.host = host
        self.port = port
        self.namespace = namespace
        self.mountpoint = mountpoint
        self.project_root = Path(__file__).parent.parent

    def log_info(self, msg: str):
        print(f"{GREEN}[INFO]{RESET} {msg}")

    def log_warn(self, msg: str):
        print(f"{YELLOW}[WARN]{RESET} {msg}")

    def log_error(self, msg: str):
        print(f"{RED}[ERROR]{RESET} {msg}")

    def is_mounted(self) -> bool:
        """Check if mountpoint is currently mounted"""
        try:
            result = subprocess.run(
                ["mountpoint", "-q", self.mountpoint],
                capture_output=True
            )
            return result.returncode == 0
        except FileNotFoundError:
            # mountpoint command not available, try /proc/mounts
            try:
                with open("/proc/mounts") as f:
                    return any(self.mountpoint in line for line in f)
            except:
                return False

    def go_run(self, cmd: str, args: list[str]) -> subprocess.CompletedProcess:
        """Run a Go command using 'go run'"""
        return subprocess.run(
            ["go", "run", f"./cmd/{cmd}", *args],
            cwd=self.project_root,
            capture_output=True,
            text=True
        )

    def unmount(self):
        """Unmount the filesystem if mounted"""
        if self.is_mounted():
            self.log_info(f"Unmounting {self.mountpoint}...")
            # Try fusermount first, then umount
            result = subprocess.run(
                ["fusermount", "-u", self.mountpoint],
                capture_output=True
            )
            if result.returncode != 0:
                subprocess.run(["umount", self.mountpoint], capture_output=True)
            time.sleep(1)

    def cleanup_mountpoint(self):
        """Remove the mountpoint directory"""
        if os.path.isdir(self.mountpoint):
            try:
                os.rmdir(self.mountpoint)
            except OSError:
                pass  # Directory not empty or other issue

    def wipe_filesystem(self):
        """Wipe existing filesystem from backend"""
        self.log_info(f"Wiping existing filesystem (namespace={self.namespace})...")
        result = self.go_run("reformat-bangfs", [
            "-host", self.host,
            "-port", self.port,
            "-namespace", self.namespace,
            "-force"
        ])
        if result.returncode != 0:
            self.log_warn("No existing filesystem to wipe (or wipe failed)")

    def create_filesystem(self):
        """Create a new filesystem in the backend"""
        self.log_info(f"Creating filesystem (namespace={self.namespace})...")
        result = self.go_run("mkfs-bangfs", [
            "-host", self.host,
            "-port", self.port,
            "-namespace", self.namespace
        ])
        if result.returncode != 0:
            self.log_error(f"Failed to create filesystem: {result.stderr}")
            raise RuntimeError("Failed to create filesystem")
        self.log_info("Filesystem created")

    def mount_filesystem(self):
        """Mount the filesystem in daemon mode"""
        self.log_info(f"Creating mountpoint {self.mountpoint}...")
        os.makedirs(self.mountpoint, exist_ok=True)

        self.log_info("Mounting filesystem in daemon mode...")
        result = self.go_run("mount-fuse-bangfs", [
            "-host", self.host,
            "-port", self.port,
            "-namespace", self.namespace,
            "-mount", self.mountpoint,
            "-daemon"
        ])
        if result.returncode != 0:
            self.log_error(f"Mount failed: {result.stderr}")
            raise RuntimeError("Mount failed")

        # Wait for mount to be ready
        time.sleep(2)

        if not self.is_mounted():
            self.log_error("Mount failed - filesystem not mounted")
            raise RuntimeError("Mount verification failed")

        self.log_info(f"Filesystem mounted at {self.mountpoint}")

    def setup(self):
        """Full setup: cleanup, create, mount"""
        self.unmount()
        self.cleanup_mountpoint()
        self.wipe_filesystem()
        self.create_filesystem()
        self.mount_filesystem()

    def teardown(self):
        """Full teardown: unmount, cleanup"""
        self.log_info("Tearing down...")
        self.unmount()
        self.cleanup_mountpoint()
        self.log_info("Teardown complete")


# ============================================================================
# MAIN
# ============================================================================

def run_tests(mount: str) -> tuple[int, int]:
    """Run all tests, return (passed, failed) counts"""
    passed = 0
    failed = 0

    for phase_name, tests in TESTS:
        print(f"\n{BLUE}{BOLD}--- {phase_name} ---{RESET}")
        for test in tests:
            if run_test(test, mount):
                passed += 1
            else:
                failed += 1

    return passed, failed


def main():
    parser = argparse.ArgumentParser(
        description="BangFS Filesystem Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s                      # Full setup, test, teardown
    %(prog)s --no-setup           # Skip setup (assume already mounted)
    %(prog)s --no-teardown        # Keep mounted after tests
    %(prog)s /mnt/custom          # Legacy mode: just run tests on path
        """
    )
    parser.add_argument("mountpoint", nargs="?", default=None,
                        help="Mountpoint (legacy mode, skips setup/teardown)")
    parser.add_argument("--no-setup", action="store_true",
                        help="Skip setup (wipe, mkfs, mount)")
    parser.add_argument("--no-teardown", action="store_true",
                        help="Skip teardown (keep filesystem mounted)")
    parser.add_argument("--host", default=os.environ.get("RIAK_HOST", DEFAULT_RIAK_HOST),
                        help=f"Riak host (default: {DEFAULT_RIAK_HOST})")
    parser.add_argument("--port", default=os.environ.get("RIAK_PORT", DEFAULT_RIAK_PORT),
                        help=f"Riak port (default: {DEFAULT_RIAK_PORT})")
    parser.add_argument("--namespace", default=os.environ.get("BANGFS_NAMESPACE", DEFAULT_NAMESPACE),
                        help=f"Filesystem namespace (default: {DEFAULT_NAMESPACE})")
    parser.add_argument("--mount", default=os.environ.get("BANGFS_MOUNTDIR", DEFAULT_MOUNTPOINT),
                        help=f"Mountpoint path (default: {DEFAULT_MOUNTPOINT})")

    args = parser.parse_args()

    # Legacy mode: just mountpoint argument
    if args.mountpoint:
        mount = args.mountpoint
        do_setup = False
        do_teardown = False
    else:
        mount = args.mount
        do_setup = not args.no_setup
        do_teardown = not args.no_teardown

    setup = BangFSSetup(args.host, args.port, args.namespace, mount)

    # Register signal handler for cleanup
    def signal_handler(sig, frame):
        print(f"\n{YELLOW}Interrupted, cleaning up...{RESET}")
        setup.teardown()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"{BOLD}BangFS Test Suite{RESET}")
    print(f"{'='*60}")
    print(f"Riak:       {args.host}:{args.port}")
    print(f"Namespace:  {args.namespace}")
    print(f"Mountpoint: {mount}")
    print(f"Setup:      {'yes' if do_setup else 'no'}")
    print(f"Teardown:   {'yes' if do_teardown else 'no'}")
    print(f"{'='*60}")

    try:
        # Setup
        if do_setup:
            setup.setup()
        else:
            # Just verify mountpoint exists
            if not os.path.isdir(mount):
                print(f"{RED}ERROR: Mountpoint {mount} does not exist{RESET}")
                sys.exit(1)

        # Run tests
        passed, failed = run_tests(mount)

        # Summary
        total = passed + failed
        print(f"\n{BOLD}{'='*60}{RESET}")
        print(f"{BOLD}RESULTS:{RESET} {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET} / {total} total")
        if failed == 0:
            print(f"{GREEN}{BOLD}ALL TESTS PASSED!{RESET}")
        else:
            pct = (passed / total) * 100 if total > 0 else 0
            print(f"Progress: {pct:.0f}% complete")
        print(f"{BOLD}{'='*60}{RESET}")

        exit_code = 0 if failed == 0 else 1

    except Exception as e:
        print(f"{RED}ERROR: {e}{RESET}")
        exit_code = 1

    finally:
        # Teardown
        if do_teardown:
            setup.teardown()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
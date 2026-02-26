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
    BANGFS_TEST_TRACE=0    # Show traces for all tests (default: only on failure)
    BANGFS_TEST_NOSKIP=0   # Don't skip remaining tests in a phase after failure
    BANGFS_TEST_PHASE=     # Run only phases matching this (e.g. "4", "Write", "4,5")

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
DIM = "\033[2m"

# Configuration defaults
DEFAULT_RIAK_HOST = "172.17.0.2"
DEFAULT_RIAK_PORT = "8087"
DEFAULT_NAMESPACE = "foobar"
DEFAULT_MOUNTPOINT = "/tmp/bangfs"
TRACE_LOG = "/tmp/bangfs-trace.log"
TEST_TRACE = os.environ.get("BANGFS_TEST_TRACE", "0") not in ("0", "", "false", "no")
TEST_NOSKIP = os.environ.get("BANGFS_TEST_NOSKIP", "0") not in ("0", "", "false", "no")
TEST_PHASE = os.environ.get("BANGFS_TEST_PHASE", "")


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
    informational: bool = False            # if True, failure doesn't skip/count


# ============================================================================
# TEST DEFINITIONS
# ============================================================================
SOME_STRING="Hello BangFS!"
SOME_OTHER_STRING="The quick brown fox jumped over the lazy dog"

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

        Test("'.' is in directory",
             r"ls -1a {mount} | grep -E '^\.$'",
             Expected.SUCCESS),

        Test("'..' is in directory",
             r"ls -1a {mount} | grep -E '^\.\.$'",
             Expected.SUCCESS),
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

        Test("exactly one instance of directory is visible",
             "test $(ls -1 '{mount}' | grep -c -E '^testdir$') -eq 1",
             Expected.SUCCESS),

        Test("stat new directory",
             "stat '{mount}/testdir'",
             Expected.SUCCESS),

        Test("ls shows new directory",
             "ls '{mount}'",
             Expected.OUTPUT_CONTAINS, "testdir"),

        Test("'.' is in directory",
             r"ls -1a {mount}/testdir | grep -E '^\.$'",
             Expected.SUCCESS),

        Test("'..' is in directory",
             r"ls -1a {mount}/testdir | grep -E '^\.\.$'",
             Expected.SUCCESS),

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

        Test("(Setup) Create a file for appending",
             "touch '{mount}/append.txt'",
             Expected.SUCCESS),

        Test("append.txt exists after touch",
             "test -f '{mount}/append.txt'",
             Expected.SUCCESS),

        Test("append.txt is empty after touch",
             "stat -c '%s' '{mount}/append.txt'",
             Expected.OUTPUT_EQUALS, "0"),

        Test("Append to append.txt",
             f"echo '{SOME_STRING}' >> '{{mount}}/append.txt'",
             Expected.SUCCESS),

        Test("append.txt has content after append",
             "cat '{mount}/append.txt'",
             Expected.OUTPUT_CONTAINS, SOME_STRING),

        Test("append.txt has correct size",
             "stat -c '%s' '{mount}/append.txt'",
             Expected.OUTPUT_EQUALS, str(len(SOME_STRING) + 1)),  # +1 for echo newline

        Test("append.txt has 1 line",
             "wc -l < '{mount}/append.txt'",
             Expected.OUTPUT_EQUALS, "1"),

        Test("Append second line to append.txt",
             f"echo '{SOME_OTHER_STRING}' >> '{{mount}}/append.txt'",
             Expected.SUCCESS),

        Test("append.txt has both lines",
             "wc -l < '{mount}/append.txt'",
             Expected.OUTPUT_EQUALS, "2"),

        Test("append.txt first line intact",
             "head -1 '{mount}/append.txt'",
             Expected.OUTPUT_CONTAINS, SOME_STRING),

        Test("append.txt second line correct",
             "tail -1 '{mount}/append.txt'",
             Expected.OUTPUT_CONTAINS, SOME_OTHER_STRING),

        Test("Cleanup append.txt",
             "rm '{mount}/append.txt'",
             Expected.SUCCESS),

        Test("append.txt gone after rm",
             "test -f '{mount}/append.txt'",
             Expected.FAIL),

        Test("echo writes to file",
             f"echo '{SOME_STRING}' > '{{mount}}/hello.txt'",
             Expected.SUCCESS),

        Test("file exists after write",
             "test -f '{mount}/hello.txt'",
             Expected.SUCCESS),

        Test("file has correct size",
             "stat -c '%s' '{mount}/hello.txt'",
             Expected.OUTPUT_EQUALS, str(len(SOME_STRING) + 1)),  # +1 for echo newline

        Test("cat reads file content",
             "cat '{mount}/hello.txt'",
             Expected.OUTPUT_CONTAINS, SOME_STRING),

        Test("echo append works",
             f"echo '{SOME_OTHER_STRING}' >> '{{mount}}/hello.txt'",
             Expected.SUCCESS),

        Test("appended content is there",
             "cat '{mount}/hello.txt'",
             Expected.OUTPUT_CONTAINS, SOME_OTHER_STRING),

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
    # PHASE 8: chmod and chown
    # -------------------------------------------------------------------------
    ("PHASE 8: chmod and chown", [
        Test("setup: create test file",
             "touch '{mount}/permtest.txt'",
             Expected.SUCCESS),

        Test("chmod 644 on file",
             "chmod 644 '{mount}/permtest.txt'",
             Expected.SUCCESS),

        Test("file has mode 644",
             "stat -c '%a' '{mount}/permtest.txt'",
             Expected.OUTPUT_EQUALS, "644"),

        Test("chmod 755 on file",
             "chmod 755 '{mount}/permtest.txt'",
             Expected.SUCCESS),

        Test("file has mode 755",
             "stat -c '%a' '{mount}/permtest.txt'",
             Expected.OUTPUT_EQUALS, "755"),

        Test("[info] chmod 000 on file",
             "chmod 000 '{mount}/permtest.txt'",
             Expected.SUCCESS, informational=True),

        Test("[info] file has mode 000",
             "stat -c '%a' '{mount}/permtest.txt'",
             Expected.OUTPUT_EQUALS, "0", informational=True),

        Test("[info] writing to mode-000 file is denied",
             "echo 'nope' > '{mount}/permtest.txt'",
             Expected.FAIL, informational=True),

        Test("[info] reading mode-000 file is denied",
             "cat '{mount}/permtest.txt'",
             Expected.FAIL, informational=True),

        Test("chmod back to 644",
             "chmod 644 '{mount}/permtest.txt'",
             Expected.SUCCESS),

        Test("[info] chmod -w removes write permission",
             "chmod -w '{mount}/permtest.txt'",
             Expected.SUCCESS, informational=True),

        Test("[info] file is mode 444 after chmod -w",
             "stat -c '%a' '{mount}/permtest.txt'",
             Expected.OUTPUT_EQUALS, "444", informational=True),

        Test("[info] writing to mode-444 file is denied",
             "echo 'nope' > '{mount}/permtest.txt'",
             Expected.FAIL, informational=True),

        Test("chmod 644 to restore",
             "chmod 644 '{mount}/permtest.txt'",
             Expected.SUCCESS),

        Test("[info] chmod +x adds execute permission",
             "chmod +x '{mount}/permtest.txt'",
             Expected.SUCCESS, informational=True),

        Test("[info] file is mode 755 after chmod +x",
             "stat -c '%a' '{mount}/permtest.txt'",
             Expected.OUTPUT_EQUALS, "755", informational=True),

        Test("[info] chmod -r removes read permission",
             "chmod -r '{mount}/permtest.txt'",
             Expected.SUCCESS, informational=True),

        Test("[info] reading mode-311 file is denied",
             "cat '{mount}/permtest.txt'",
             Expected.FAIL, informational=True),

        Test("cleanup permtest.txt",
             "chmod 644 '{mount}/permtest.txt'; rm '{mount}/permtest.txt'",
             Expected.SUCCESS),

     # TODO: fix this test, it should succeed
     #    Test("chmod 000 on file",
     #         "chmod 000 '{mount}/permtest.txt'",
     #         Expected.SUCCESS),

     #    Test("file has mode 000",
     #         "stat -c '%a' '{mount}/permtest.txt'",
     #         Expected.OUTPUT_EQUALS, "0"),

     #    Test("chmod back to 644",
     #         "chmod 644 '{mount}/permtest.txt'",
     #         Expected.SUCCESS),

     #    Test("setup: create test dir",
     #         "mkdir '{mount}/permdir'",
     #         Expected.SUCCESS),

     #    Test("chmod 700 on directory",
     #         "chmod 700 '{mount}/permdir'",
     #         Expected.SUCCESS),

     #    Test("dir has mode 700",
     #         "stat -c '%a' '{mount}/permdir'",
     #         Expected.OUTPUT_EQUALS, "700"),

     #    Test("chown to current user",
     #         "chown $(id -u):$(id -g) '{mount}/permtest.txt'",
     #         Expected.SUCCESS),

     #    Test("file uid matches",
     #         "test $(stat -c '%u' '{mount}/permtest.txt') -eq $(id -u)",
     #         Expected.SUCCESS),

     #    Test("file gid matches",
     #         "test $(stat -c '%g' '{mount}/permtest.txt') -eq $(id -g)",
     #         Expected.SUCCESS),

     #    Test("cleanup permtest.txt",
     #         "rm '{mount}/permtest.txt'",
     #         Expected.SUCCESS),

     #    Test("cleanup permdir",
     #         "rmdir '{mount}/permdir'",
     #         Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 9: Edge Cases and Error Handling
    # -------------------------------------------------------------------------
    ("PHASE 9: Edge Cases and Error Handling", [
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

        Test("hardlink is not supported",
             "touch '{mount}/hlsrc' && ln '{mount}/hlsrc' '{mount}/hldst'",
             Expected.FAIL),

        Test("cleanup hardlink test",
             "rm -f '{mount}/hlsrc' '{mount}/hldst'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 10: Overwrite and Truncate
    # -------------------------------------------------------------------------
    ("PHASE 10: Overwrite and Truncate", [
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

    # -------------------------------------------------------------------------
    # PHASE 11: Random Write and Seek
    # -------------------------------------------------------------------------
    ("PHASE 11: Random Write and Seek", [
        # Setup: create a file with known content (20 bytes: AAAAA BBBBB CCCCC DDDDD)
        Test("setup: create 20-byte file",
             "printf 'AAAAABBBBBCCCCCDDDDD' > '{mount}/seek.txt'",
             Expected.SUCCESS),

        Test("verify initial content",
             "cat '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "AAAAABBBBBCCCCCDDDDD"),

        Test("verify initial size",
             "stat -c '%s' '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "20"),

        # Random write in the middle: overwrite bytes 5-9 with XXXXX
        Test("dd write at offset 5",
             "echo -n 'XXXXX' | dd of='{mount}/seek.txt' bs=1 seek=5 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("middle overwrite: content correct",
             "cat '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "AAAAAXXXXXCCCCCDDDDD"),

        Test("middle overwrite: size unchanged",
             "stat -c '%s' '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "20"),

        # Random write at start: overwrite bytes 0-2 with ZZZ
        Test("dd write at offset 0",
             "echo -n 'ZZZ' | dd of='{mount}/seek.txt' bs=1 seek=0 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("start overwrite: content correct",
             "cat '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "ZZZAAXXXXXCCCCCDDDDD"),

        # Random write at end: overwrite last 3 bytes
        Test("dd write at offset 17",
             "echo -n '!!!' | dd of='{mount}/seek.txt' bs=1 seek=17 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("end overwrite: content correct",
             "cat '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "ZZZAAXXXXXCCCCCDD!!!"),

        Test("end overwrite: size unchanged",
             "stat -c '%s' '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "20"),

        # Read at specific offset with dd
        Test("dd read 5 bytes at offset 5",
             "dd if='{mount}/seek.txt' bs=1 skip=5 count=5 2>/dev/null",
             Expected.OUTPUT_EQUALS, "XXXXX"),

        Test("dd read 3 bytes at offset 0",
             "dd if='{mount}/seek.txt' bs=1 skip=0 count=3 2>/dev/null",
             Expected.OUTPUT_EQUALS, "ZZZ"),

        # Write past end of file (extends file with gap)
        Test("dd write past EOF extends file",
             "echo -n 'PAST' | dd of='{mount}/seek.txt' bs=1 seek=25 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("file grew after write past EOF",
             "stat -c '%s' '{mount}/seek.txt'",
             Expected.OUTPUT_EQUALS, "29"),

        Test("content at offset 25 is correct",
             "dd if='{mount}/seek.txt' bs=1 skip=25 count=4 2>/dev/null",
             Expected.OUTPUT_EQUALS, "PAST"),

        Test("cleanup seek.txt",
             "rm '{mount}/seek.txt'",
             Expected.SUCCESS),
    ]),

    # -------------------------------------------------------------------------
    # PHASE 12: Random Write in Large (multi-chunk) Files
    # -------------------------------------------------------------------------
    ("PHASE 12: Random Write in Large Files", [
        # chunksize is 10240 bytes, so a 30KB file spans 3 chunks
        # Create a 30KB file filled with 'A' (0x41)
        Test("setup: create 30KB file of A's",
             "dd if=/dev/zero bs=1 count=30720 2>/dev/null | tr '\\0' 'A' > '{mount}/bigseek.bin'",
             Expected.SUCCESS),

        Test("30KB file has correct size",
             "stat -c '%s' '{mount}/bigseek.bin'",
             Expected.OUTPUT_EQUALS, "30720"),

        # Verify first and last bytes are 'A'
        Test("first byte is A",
             "dd if='{mount}/bigseek.bin' bs=1 count=1 2>/dev/null",
             Expected.OUTPUT_EQUALS, "A"),

        Test("last byte is A",
             "dd if='{mount}/bigseek.bin' bs=1 skip=30719 count=1 2>/dev/null",
             Expected.OUTPUT_EQUALS, "A"),

        # Write in the middle of chunk 1 (offset 5000)
        Test("write HELLO at offset 5000 (chunk 1 interior)",
             "echo -n 'HELLO' | dd of='{mount}/bigseek.bin' bs=1 seek=5000 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("read back HELLO at offset 5000",
             "dd if='{mount}/bigseek.bin' bs=1 skip=5000 count=5 2>/dev/null",
             Expected.OUTPUT_EQUALS, "HELLO"),

        Test("size unchanged after chunk 1 write",
             "stat -c '%s' '{mount}/bigseek.bin'",
             Expected.OUTPUT_EQUALS, "30720"),

        # Write across chunk boundary (chunk 1 ends at 10240, write 10 bytes at 10235)
        Test("write CROSSBOUND at chunk 1/2 boundary (offset 10235)",
             "echo -n 'CROSSBOUND' | dd of='{mount}/bigseek.bin' bs=1 seek=10235 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("read back CROSSBOUND at offset 10235",
             "dd if='{mount}/bigseek.bin' bs=1 skip=10235 count=10 2>/dev/null",
             Expected.OUTPUT_EQUALS, "CROSSBOUND"),

        Test("bytes before boundary write untouched",
             "dd if='{mount}/bigseek.bin' bs=1 skip=10230 count=5 2>/dev/null",
             Expected.OUTPUT_EQUALS, "AAAAA"),

        Test("bytes after boundary write untouched",
             "dd if='{mount}/bigseek.bin' bs=1 skip=10245 count=5 2>/dev/null",
             Expected.OUTPUT_EQUALS, "AAAAA"),

        # Write in chunk 3 (offset 25000)
        Test("write CHUNK3 at offset 25000",
             "echo -n 'CHUNK3' | dd of='{mount}/bigseek.bin' bs=1 seek=25000 conv=notrunc 2>/dev/null",
             Expected.SUCCESS),

        Test("read back CHUNK3 at offset 25000",
             "dd if='{mount}/bigseek.bin' bs=1 skip=25000 count=6 2>/dev/null",
             Expected.OUTPUT_EQUALS, "CHUNK3"),

        # Verify earlier writes are still intact
        Test("HELLO still at offset 5000",
             "dd if='{mount}/bigseek.bin' bs=1 skip=5000 count=5 2>/dev/null",
             Expected.OUTPUT_EQUALS, "HELLO"),

        Test("CROSSBOUND still at offset 10235",
             "dd if='{mount}/bigseek.bin' bs=1 skip=10235 count=10 2>/dev/null",
             Expected.OUTPUT_EQUALS, "CROSSBOUND"),

        Test("size still 30720 after all writes",
             "stat -c '%s' '{mount}/bigseek.bin'",
             Expected.OUTPUT_EQUALS, "30720"),

        # Large read spanning multiple chunks
        Test("read 20 bytes spanning chunk 1/2 boundary",
             "dd if='{mount}/bigseek.bin' bs=1 skip=10230 count=20 2>/dev/null",
             Expected.OUTPUT_EQUALS, "AAAAACROSSBOUNDAAAAA"),

        Test("cleanup bigseek.bin",
             "rm '{mount}/bigseek.bin'",
             Expected.SUCCESS),
    ]),
]


# ============================================================================
# TEST RUNNER
# ============================================================================

class TraceReader:
    """Reads new lines from the trace log file between tests.

    Captures trace output and can either print immediately (flush)
    or buffer it for printing only on failure (drain/dump).
    """

    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        self.buffer: list[str] = []
        # Truncate any stale log from previous runs
        try:
            open(path, "w").close()
        except OSError:
            pass

    def _read_new(self) -> list[str]:
        """Read new lines from the trace log since last call."""
        try:
            with open(self.path, "r") as f:
                f.seek(self.pos)
                lines = f.readlines()
                self.pos = f.tell()
        except OSError:
            return []
        return [l.rstrip() for l in lines if l.strip()]

    def capture(self):
        """Read new trace lines into the buffer (don't print yet)."""
        self.buffer.extend(self._read_new())

    def flush(self):
        """Print any new trace lines immediately, and clear the buffer."""
        self.buffer.extend(self._read_new())
        self._print_buffer()

    def dump(self):
        """Print whatever is in the buffer, then clear it."""
        self.capture()
        self._print_buffer()

    def discard(self):
        """Discard buffered trace lines without printing."""
        self.capture()
        self.buffer.clear()

    def _print_buffer(self):
        for line in self.buffer:
            print(f"       {DIM}{line}{RESET}")
        self.buffer.clear()


# Global trace reader, initialized during setup
trace_reader: Optional[TraceReader] = None


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
    elif test.informational:
        print(f"  {BLUE}INFO{RESET} {test.description}")
        if details:
            print(f"       {BLUE}{details}{RESET}")
    else:
        print(f"  {RED}FAIL{RESET} {test.description}")
        if details:
            print(f"       {RED}{details}{RESET}")

    # Show trace output: always on TEST_TRACE, otherwise only on failure
    if trace_reader:
        if TEST_TRACE:
            trace_reader.flush()
        elif not passed:
            trace_reader.dump()
        else:
            trace_reader.discard()

    # Run cleanup if present
    if test.cleanup:
        run_command(test.cleanup.format(mount=mount))

    return passed


# ============================================================================
# SETUP / TEARDOWN
# ============================================================================

class BangFSSetup:
    """Handles setup and teardown of BangFS for testing"""

    def __init__(self, host: str, port: str, namespace: str, mountpoint: str, dummy: bool = False):
        self.host = host
        self.port = port
        self.namespace = namespace
        self.mountpoint = mountpoint
        self.dummy = dummy
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

    def _backend_args(self) -> list[str]:
        """Return backend flags: either -dummy or -host/-port"""
        if self.dummy:
            return ["-dummy"]
        return ["-host", self.host, "-port", self.port]

    def wipe_filesystem(self):
        """Wipe existing filesystem from backend"""
        self.log_info(f"Wiping existing filesystem (namespace={self.namespace})...")
        result = self.go_run("reformat-bangfs", [
            *self._backend_args(),
            "-namespace", self.namespace,
            "-force"
        ])
        if result.returncode != 0:
            self.log_warn("No existing filesystem to wipe (or wipe failed)")

    def create_filesystem(self):
        """Create a new filesystem in the backend"""
        self.log_info(f"Creating filesystem (namespace={self.namespace})...")
        result = self.go_run("mkfs-bangfs", [
            *self._backend_args(),
            "-namespace", self.namespace
        ])
        if result.returncode != 0:
            self.log_error(f"Failed to create filesystem: {result.stderr}")
            raise RuntimeError("Failed to create filesystem")
        self.log_info("Filesystem created")

    def mount_filesystem(self):
        """Mount the filesystem in daemon mode"""
        global trace_reader
        self.log_info(f"Creating mountpoint {self.mountpoint}...")
        os.makedirs(self.mountpoint, exist_ok=True)

        trace_reader = TraceReader(TRACE_LOG)

        self.log_info("Mounting filesystem in daemon mode...")
        mount_args = [
            *self._backend_args(),
            "-namespace", self.namespace,
            "-mount", self.mountpoint,
            "-daemon",
            "-trace",
            "-tracelog", TRACE_LOG,
        ]
        result = self.go_run("mount-fuse-bangfs", mount_args)
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
        """Full setup: create, mount"""
        self.create_filesystem()
        self.mount_filesystem()

    def teardown(self):
        """Full teardown: unmount"""
        self.log_info("Tearing down...")
        self.unmount()
        # self.cleanup_mountpoint()
        self.wipe_filesystem()
        self.log_info("Teardown complete")


# ============================================================================
# MAIN
# ============================================================================

def run_preflight(mount: str) -> bool:
    """Run preflight checks. Returns True if all pass, False otherwise."""
    checks = [
        ("mounted as FUSE filesystem", f"stat -f -c '%T' '{mount}'", "fuseblk"),
        ("mounted as bangfs in /proc/mounts", f"grep '{mount} ' /proc/mounts", "fuse.bangfs"),
        ("ls on mountpoint works", f"ls '{mount}'", None),
    ]
    print(f"\n{BLUE}{BOLD}--- Preflight ---{RESET}")
    for desc, cmd, expected_substr in checks:
        ok, stdout, stderr = run_command(cmd)
        if not ok:
            print(f"  {RED}FAIL{RESET} {desc}")
            print(f"       {RED}{stderr or 'command failed'}{RESET}")
            if trace_reader:
                trace_reader.dump()
            return False
        if expected_substr and expected_substr not in stdout:
            print(f"  {RED}FAIL{RESET} {desc}")
            print(f"       {RED}expected '{expected_substr}' in output, got: '{stdout}'{RESET}")
            if trace_reader:
                trace_reader.dump()
            return False
        print(f"  {GREEN}PASS{RESET} {desc}")
        if trace_reader:
            if TEST_TRACE:
                trace_reader.flush()
            else:
                trace_reader.discard()
    return True

def _phase_matches(phase_name: str, phase_filter: str) -> bool:
    """Check if a phase name matches the filter. Filter can be comma-separated."""
    if not phase_filter:
        return True
    for term in phase_filter.split(","):
        term = term.strip()
        if not term:
            continue
        if term.lower() in phase_name.lower():
            return True
    return False

def run_tests(mount: str, phase_filter: str = "") -> tuple[int, int, int]:
    """Run all tests, return (passed, failed, skipped) counts"""
    passed = 0
    failed = 0
    skipped = 0

    for phase_name, tests in TESTS[:]:
        if not _phase_matches(phase_name, phase_filter):
            continue
        print(f"\n{BLUE}{BOLD}--- {phase_name} ---{RESET}")
        phase_failed = False
        for test in tests:
            is_cleanup = test.description.lower().startswith("cleanup")
            if phase_failed and not TEST_NOSKIP and not is_cleanup:
                print(f"  {YELLOW}SKIP{RESET} {test.description}")
                skipped += 1
                continue
            if run_test(test, mount):
                passed += 1
            elif test.informational:
                pass  # don't count, don't skip following tests
            else:
                failed += 1
                phase_failed = True

    return passed, failed, skipped


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
    parser.add_argument("--dummy", action="store_true", help="Use file-backed store under /tmp instead of Riak")
    parser.add_argument("--phase", default=TEST_PHASE,
                        help="Run only phases matching this string (e.g. '4', 'Write', '4,5'). Env: BANGFS_TEST_PHASE")

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

    setup = BangFSSetup(args.host, args.port, args.namespace, mount, dummy=args.dummy)

    # Register signal handler for cleanup
    def signal_handler(sig, frame):
        print(f"\n{YELLOW}Interrupted, cleaning up...{RESET}")
        setup.teardown()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"{BOLD}BangFS Test Suite{RESET}")
    print(f"{'='*60}")
    if args.dummy:
        print(f"Backend:    file (/tmp/bangfs_{args.namespace}/)")
    else:
        print(f"Backend:    Riak ({args.host}:{args.port})")
    print(f"Namespace:  {args.namespace}")
    print(f"Mountpoint: {mount}")
    print(f"Setup:      {'yes' if do_setup else 'no'}")
    print(f"Teardown:   {'yes' if do_teardown else 'no'}")
    print(f"{'='*60}")

    try:
        # Setup
        if do_setup:
            setup.setup()

        # Ensure trace reader is initialized even without setup
        global trace_reader
        if trace_reader is None:
            trace_reader = TraceReader(TRACE_LOG)

        # Preflight: all three must pass or we abort
        if not run_preflight(mount):
            print(f"\n{RED}Preflight failed — aborting.{RESET}")
            sys.exit(1)

        # Run tests
        passed, failed, skipped = run_tests(mount, phase_filter=args.phase)

        # Summary
        total = passed + failed + skipped
        print(f"\n{BOLD}{'='*60}{RESET}")
        print(f"{BOLD}RESULTS:{RESET} {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET}, {YELLOW}{skipped} skipped{RESET} / {total} total")
        if failed == 0 and skipped == 0:
            print(f"{GREEN}{BOLD}ALL TESTS PASSED!{RESET}")
        else:
            pct = (passed / total) * 100 if total > 0 else 0
            print(f"Progress: {pct:.0f}% complete")
        print(f"{BOLD}{'='*60}{RESET}")

        exit_kode = 0 if failed == 0 else 1

    except Exception as e:
        print(f"{RED}ERROR: {e}{RESET}")
        exit_kode = 1

    finally:
        # Teardown
        if do_teardown:
            setup.teardown()

    sys.exit(exit_kode)


if __name__ == "__main__":
    main()
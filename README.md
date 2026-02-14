# BangFS

## Motivation

Build a novel distributed filesystem out of available parts, insipired by GFS, and not a few "System Design/Architecture" interviews.

## Design Goals

- Be as homogenous as possible, avoid explicit 'leader-follower' patterns and use as few different services as possible.
- Be scaleable, in theory.
- Be useable at least at small scale, with latency and performance characteristics at least as good as spinning disks.

## Other goals

- Deployable on Kubernetes, because.

## High Level Concept

- store chunks of files in a distributed, eventually consistent, key value store, indexed by hash of the content
- store metadata in a strongly consistent way

## Design issues/shortcomings/TODOS

_Probably nobody would actually build a production system this way._ 

- Unique Inode number generation in a distributed system (needed for metadata)
- If chunks are referenced by hash of the content (ie, are deduplicated), how to track what chunks are used by what files in a consistent way? ie, if you delete a file, how to determine what chunks to keep or delete?
- etc.

# Digby: A Rust-based Key-Value Store

Digby is an embedded key-value store written in Rust. It was built as a project to learn Rust and explore advanced database implementation concepts, utilizing a B+ Tree as its core data structure.


## Features

*   **B+ Tree Core**: Uses a robust B+ tree for storing key-value pairs.
*   **Global & Table-based Stores**: Supports a root global B+ tree as well as independent B+ trees (tables), all stored in a single file.
*   **Large Item Support**: Capable of storing large keys and values (up to 64-bit sizes).
    *   Large items are stored using overflow pages and can optionally be compressed with LZ4 (conceptually similar to TOAST in PostgreSQL).
    *   Large keys are indexed using a combination of their prefix (first 223 bytes) and a SHA-256 hash (32 bytes), allowing lexical sorting up to 223 bytes.
    *   Overflow pages can be up to 1MB in size and chain together to support larger objects, optimized to minimize page reads.
*   **Configurable Block & Page Sizes**:
    *   The base block size is configurable (e.g., 4K for typical Linux atomic write compatibility).
    *   Pages consist of one or more blocks, sized in powers of two (4K, 8K, 16K... 1024K).
    *   Tree directory pages and leaf page sizes can be set independently.
    *   Key metadata pages (root page, master pages, free directory pages) are fixed at a single block.
    *   Overflow pages use the minimum number of blocks required (similar to ZFS), managed by a slab allocator.
*   **Copy-On-Write (COW)**: Implements shadowing/clones based on the *"B-trees, Shadowing, and Clones"* paper, similar to the approaches used in ZFS, Bcachefs, and LMDB.
*   **Simplified Deletion**: Implements deletion without requiring complex tree rebalancing, based on the *"Deletion Without Rebalancing in Multiway Search Trees"* paper.
*   **Data Integrity and Security**:
    *   **Checksums**: Option to use either `xxhash32` (32 bits) or `xxhash3` (64 bits) for page integrity verification.
    *   **Encryption**: Optional AES-128-GCM encryption for all stored content, leveraging its built-in cryptographic integrity checks.
*   **Compression**: 
    *   Head and tail compression in B+ tree nodes based on the SIGMOD '24 paper *"B-tree Compression"*.
    *   Optional LZ4 compression for large keys and values.
*   **Large Scale**: 64-bit page numbers support extremely large databases (56 bits for effective addressing, 4 bits used to encode page block count and 4 bits for page type).
*   **Transactions**: Supports ACID transactions to make multiple atomic changes isolated from readers. Currently supports a single concurrent writer (RCU-style via COW) with durable updates synced to disk.

## Getting Started

To build the project, ensure you have Rust installed and run:

```sh
cargo build
```

To execute the test suite:

```sh
cargo test
```

## Architecture & Design Explorations

### Transactions: COW vs. ARIES
Copy-On-Write (COW) is used in filesystems like ZFS and Bcachefs, as well as databases like LMDB. In older literature (e.g., System R), this approach is known as "page shadowing." The alternative approach is ARIES, a Write-Ahead Logging (WAL) protocol with redo and undo phases for recovery. COW works well for filesystems as they typically do not need to support complex transactions. LMDB, which also uses COW, is designed for high read volume and low write volume (LDAP) with a single writer.

Within Digby, transactions are supported via `_txn` methods. The client starts a transaction with `db.new_transaction` and passes the transaction context to subsequent operations. When ready, the client calls `db.commit`. Operations modify the tree during the transaction but do not update the master page until the commit. 

Because Digby uses COW, it naturally supports multiple readers that do not block each other or the writer, but it restricts writes to a single concurrent writer. Readers can use version information in pages/tuples to detect stale state and retry. Supporting complex transactions with multiple concurrent writers and rollbacks would likely require an ARIES-type approach. *Open Question: If switching to an ARIES approach, can the log be efficiently maintained in the same file as the tree?*

### Checksums and Merkle Trees
Both ZFS and Bcachefs store the checksum for a page in the pointer to the page/object rather than in the page itself, except for the root node. This forms a Merkle tree (similar to Git) and catches complex errors like phantom writes, misdirected I/O, and DMA parity errors better than simple bit rot checks. 

In Digby, embedding the checksum in the page pointer presents challenges:
*   **Checksum Size**: A fixed size (e.g., 32-bit `xxhash32` or 64-bit `xxhash3`) would be needed. 
*   **Encryption Overlay**: Digby supports AES-128-GCM, which has built-in cryptographic hashing and requires a 96-bit nonce. Relying on AES-128-GCM avoids duplicate checksum work.
*   **Space Overhead**: Storing checksums in pointers consumes more space in internal directory nodes.

To fully support a Merkle tree in Digby, an uncontroversial checksum size (e.g., 64-bit `xxhash3`) would be required. If encryption were also enabled, we would pay the price of double-checksumming. The page pointer would expand to 128 bits (64 bits for addressing + 64 bits for the checksum).

### Fast Paxos & Flexible Paxos
Integrating Paxos into the database could provide an interesting alternative to a traditional WAL. For instance, if Paxos outputs a queue of agreed work, this could serve as the transaction log. Fast Paxos can reach agreement in a single round but suffers under high contention, requiring larger quorums. Flexible Paxos helps mitigate phase 2 quorum bottlenecks.

In a sharded architecture, Digby could replicate thousands of Paxos state machines by partitioning the key namespace. For example, 2,000 state machines could map to 2,000 independent B+ trees rooted in a single file, each utilizing COW. This could heavily leverage the parallel I/O capabilities of NVMe drives, with cross-shard transactions utilizing Paxos Commit. *Open Question: Would this architecture lose the natural range-query advantages of a B+ tree?*

## Future Explorations

*   **Top-Down Tree Updates**: Digby currently uses a bottom-up approach to writing nodes (leaf -> directory -> master page). This is sub-optimal compared to the top-down approach advocated in *"B-trees, Shadowing, and Clones,"* which writes directory pages on the way down, using preemptive splits to allow better parallelism.
*   **Per-Table Page Sizes**: Allow different B+ trees (tables) within the same database to configure distinct directory and leaf page sizes.
*   **MVCC (Multi-Version Concurrency Control)**: Extend the existing rudimentary versioning system to support more complex concurrent transactions.
*   **Performance Optimizations**:
    *   **Page Cache**: Enhance the rudimentary page cache and reduce unnecessary page copying when dealing with encrypted pages.
    *   **Asynchronous I/O**: Investigate `io_uring` (via Rust's `tokio`) for async I/O. Currently, new pages overwrite existing free pages synchronously, followed by a double `sync_data` around the master page write. `io_uring` could schedule page write-backs and `sync_file_range` asynchronously, waiting on the batch before writing the master page.
    *   **Log-Structured Leaves**: Explore update optimizations similar to Bcachefs (e.g., logging changes into 256K leaf page chunks and compacting/splitting them when full) to reduce the write amplification of standard COW B-trees.
*   **Concurrency**: Add support for multi-threaded access. The current COW design supports a single writer and multiple readers. Moving to top-down tree writing would be the first step toward better concurrent writer scaling.
*   **Untorn Writes**: Investigate leveraging Linux untorn writes (atomic writes of multiple aligned blocks, like 16K on NVMe SSDs). This avoids the double-write penalty of traditional WALs. MySQL saw performance degradation with 16K untorn writes due to write amplification on its 512-byte log blocks, so integrating this effectively into Digby requires careful design.
*   **Direct NVMe Access**: Explore bypassing the filesystem to access NVMe as a raw KV store for Digby blocks (e.g., referencing *"SAKER: A Software Accelerated Key-value Service via the NVMe Interface"*).
*   **Code Quality**: Continually refactor for more idiomatic Rust.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

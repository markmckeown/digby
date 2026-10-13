# Digby: A Rust-based Key-Value Store

Digby is an embedded key-value store written in Rust. It was built as a project to learn Rust and explore database implementation concepts. It uses a Copy on Write (COW) B+ Tree as its core data structure.


## Features

*   **B+ Tree Core**: Uses a B+ tree for storing key-value pairs.
*   **Global & Table-based Stores**: Supports a root global B+ tree as well as independent B+ trees (tables), all stored in a single file.
*   **Large Item Support**: Capable of storing large keys and values (up to 64-bit sizes).
    *   Large items are stored using overflow pages and can optionally be compressed with LZ4 (conceptually similar to TOAST in PostgreSQL).
    *   Large keys are indexed using a combination of their prefix (first 223 bytes) and a SHA-256 hash (32 bytes), allowing lexical sorting up to 223 bytes.
    *   Overflow pages can be up to 1MB in size and chain together to support larger objects, optimized to minimize page reads rather than pack data.
*   **Configurable Block & Page Sizes**:
    *   The base block size is configurable (e.g., 4K for typical Linux atomic write compatibility).
    *   Pages consist of one or more blocks, sized in powers of two (4K, 8K, 16K... 1024K).
    *   Tree directory pages and leaf page sizes can be set independently.
    *   Key metadata pages (root page, master pages, free directory pages) are fixed at a single block.
    *   Overflow pages use the minimum number of blocks required (similar to ZFS), managed by a slab allocator.
*   **Copy-On-Write (COW)**: Implements shadowing/clones based on the *"B-trees, Shadowing, and Clones"* paper, similar to the approaches used in ZFS, Bcachefs, and LMDB.
*   **Simplified Deletion**: Implements deletion without tree rebalancing, based on the *"Deletion Without Rebalancing in Multiway Search Trees"* paper.
*   **Data Integrity and Security**:
    *   **Checksums**: Option to use either `xxhash32` (32 bits) or `xxhash3` (64 bits) for page integrity verification.
    *   **Encryption**: Optional AES-128-GCM encryption for all stored content, leveraging its built-in cryptographic integrity checks.
    *   **Embedded Physical Identity**: Detects mis-directed writes and allows recovery if using a mirror, based on *"Parity Lost and Parity Regained"*.
    *   **Page Version Mirror**: Detects lost-writes and allows recovery if using a mirror, based on *"Parity Lost and Parity Regained"*.
    *   **Built-in RAID-1/Mirror**: Support for a mirror of the database where all blocks are duplicated on write to another file within COW. Allows automatic repair of page corruption, lost writes, and mis-directed writes similar to ZFS.
*   **Compression**: 
    *   Head and tail compression in B+ tree nodes per *"B-trees Are Back: Engineering Fast and Pageable Node Layouts"* and *"An Evaluation of B-tree Compression Techniques".*
    *   Optional LZ4 compression for large keys and values.
*   **Large Scale**: 64-bit page numbers support extremely large databases (48 bits for block addressing, 4 bits used to encode page block count exponent, 4 bits for page type, and 8 bits for page mirror version). With 4K blocks this allows a 1EiB database.
*   **Transactions**: Supports ACID transactions to make multiple atomic changes isolated from readers. Currently supports a single concurrent writer (RCU-style via COW) with durable updates synced to disk. Atomicity and durability are achieved by committing updates through an atomic master page update.

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

Because Digby uses COW, it naturally supports multiple readers that do not block each other or the writer, but it restricts writes to a single concurrent writer. Readers can use version information in pages/tuples to detect stale state and retry, and it would also be possible to add support for multiple snapshots so a reader never encounters stale state. Supporting complex transactions with multiple concurrent writers and rollbacks would likely require an ARIES-type approach. If switching to an ARIES approach, can the log be efficiently maintained in the same file as the tree? 

### Checksums and Merkle Trees
Both ZFS and Bcachefs store the checksum for a page in the pointer to the page/object rather than in the page itself, except for the superblock. This forms a Merkle tree (similar to Git) and catches complex errors like lost or phantom writes, misdirected I/O, and DMA parity errors better than simple bit rot checks. Bcachefs stores checksums for the blocks in a separate tree; this causes write amplification. 

Is it a challenge for a FS to store the checksum in the page, making serving the pages to clients tricky?

In Digby, embedding the checksum in the page pointer presents challenges:
*   **Checksum Size**: A fixed size (e.g., 32-bit `xxhash32` or 64-bit `xxhash3`) would be needed. 
*   **Encryption Overlay**: Digby supports AES-128-GCM, which has built-in cryptographic hashing and requires a 96-bit nonce. Relying on AES-128-GCM avoids duplicate checksum work.
*   **Space Overhead**: Storing checksums in pointers consumes more space in internal directory nodes.

Another challenge with embedding the checksum in the page pointer is that it prevents the optimizations in *"B-trees, Shadowing, and Clones"* - in this approach as you descend the tree you write out new pages, i.e., directory pages, including proactively splitting directory nodes. This allows crabbing the locks as you descend the tree. However, if embedding the checksum in the page pointer, directory entries cannot be created because the checksum for the child page is not known until the child page is created.

Digby uses an approach similar to that outlined in *"Parity Lost and Parity Regained"*. The page pointer contains the page type encoded as 4 bits, the number of blocks that make up the page as a power of two shift encoded as 4 bits, a version number encoded as a byte that is incremented each time the page is written called the parity, and 48 bits for the block offset. The page number is also stored into the page, meaning the page number is stored in two locations. This means the page contains a checksum to detect corruption, a physical identity to detect misdirected writes, and a version mirror to detect lost writes (the version is mirrored in the page and in the page pointer stored in the parent page). The version is encoded as a byte but could be encoded as a single bit; this is possible as the page has to be read by a client to be written. In a disk RAID, the page could be read during a scrub and a single bit is not enough (see *"Parity Lost and Parity Regained"*).

Using this approach, Digby can detect corrupt pages (checksum), lost writes (page parity version), and misdirected writes (physical identity), making it as robust as a Merkle tree without the disadvantages outlined above. With a mirror it can recover from these errors and detect double failures (both primary and mirror corrupt for a page).

With 48 bits for addressing and 4K blocks, the size limit for Digby is 1EiB.

### Snapshots
It is relatively easy to take a clone/snapshot of a COW B-tree, the tricky part is to know which blocks can be reused after a snapshot is deleted. Note the Btrfs paper points out a difference in functionality between itself and ZFS, in both you can make a clone of a filesystem, a writable snapshot, but in Btrfs you can clone a clone while in ZFS you cannot.

*"B-trees, Shadowing, and Clones"* outlines an approach for efficiently taking a snapshot of a B-tree: when you take a snapshot you increment the reference count of the child nodes of the root of the tree. You do not initially need to increment the reference count of any other nodes; there is a set of rules for when the reference count for other nodes can be incremented or decremented. Btrfs uses this approach. 
Btrfs uses a B-tree called the extent tree to track block reference counts, see https://josefbacik.github.io/kernel/btrfs/2021/12/16/btrfs-extent-reference-counting.html - Btrfs reference counts every block so there is a lot of write amplification. It's not clear from *"B-trees, Shadowing, and Clones"* whether a form of sparse reference counting could be used whereby entries for blocks are only counted when the reference count goes above one (see *"Efficient Free Space Reclamation in WAFL"*).

Filesystems have traditionally used bitmaps to track free blocks. In the NetApp WAFL filesystem they initially used 32 bits instead of a bit to support up to 32 snapshots for a volume. A bit set in the 32 bits would indicate the block was in use in the corresponding snapshot - if no bits were set then the block was free, the approach is described in *"File System Design for an NFS File Server Appliance"*. This approach did not scale and was replaced by a different approach: a volume has a bitmap (called the active map) tracking which blocks are free with one bit per 4K block, updated using copy-on-write. When a snapshot is taken the active map now becomes a snap map and is no longer changed in the snapshot (COW); to determine if a block is free, for example when a snapshot is deleted, the active map and snap maps are OR'd to see if the bit is still set in any of the snapshots or the current volume.

ZFS takes a different approach: in each block pointer it records the birthday of the block, the transaction number that first used the block. When a snapshot is deleted, a delta is taken of the previous and next snapshot to work out which blocks from the snapshot can be reused. See https://papers.freebsd.org/2019/bsdcan/ahrens-how_zfs_snapshots_really_work/ The paper *"BTRFS: The Linux B-tree Filesystem"* makes the case that snapshots in ZFS are not a first-class citizen, you cannot clone a clone in ZFS.

The Bcachefs approach is described here: https://bcachefs.org/Snapshots/.

As of version 0.6, Digby has two snapshots: the current and previous, these are managed by the two master pages (current and previous). When an update is started, the current master page is copied and used to find the tree roots, new blocks are written out and a set of no longer used blocks is collected. When the transaction commits, the freed blocks are added to the free lists. The previous master page is overwritten with the new master copy (previous now becomes current) leaving two versions of the DB. We can extend this to N versions: when a transaction completes, write the freed blocks to a ring buffer of pages in an object with the transaction id (reusing a tuple). When we have completed N transactions we can remove the freed blocks from the ring buffer and add them to the free page lists, and the first snapshot is no longer available; iterate forward in this manner. It's not clear if clone (writable) snapshots can be created with this approach. Note also that each page has a version number, similar to ZFS, so a variation of the ZFS approach could be used.

Note that creating a ring buffer of pages or a doubly linked list of pages is difficult in COW. When a page needs to be modified, a new version is created and written at a different location; this means anything that pointed to this page now has a broken pointer. An approach to addressing this is to use a method based on Bcachefs: use a tree to store the pages that can be freed in the future. The leaf pages in this case will be written using an LSM-like approach; pages that can be freed are appended to the leaf page by treating the leaf page as a log. The leaf page could be 256K, e.g., 64 blocks of 4K (with directory nodes being 1 block), each update uses the next block in the page and when the page is filled it can be compacted and written out to disk using COW. When the page is full after compaction a new page can be added to the table. This would allow a ring buffer to be implemented using a tree with LSM nodes. Care would be needed to maintain fault detection and repair: page corruption, lost writes, and redirected writes.


### Log Leaf Pages Use Cases

In the previous section there was discussion of implementing a ring buffer using leaf pages similar to Bcachefs. The leaf page is treated like a log similar to an LSM tree. 

The leaf page is treated as a log with entries appended to it, no data in the page is overwritten or replaced, and the normal COW process is not used. When the page is filled, it could be compacted and rewritten using the normal COW process, or a new leaf page could be added to the tree, or the page could be split again using COW. The size of the log page could be 1MB using existing supported page container sizes, or 128MB if using all the available address space for page containers.

There are a number of use cases that could be efficiently addressed using this approach.

Snapshot management. This is discussed above. The log leaf pages could act as a ring buffer implemented using a B-tree with log leaf pages. Each entry is the transaction id and the value of the entry is the page numbers that are obsolete from the transaction. The transaction id is an increasing 64-bit number, so the tree grows to the right - so when a page fills, rather than splitting, we add a page to the right and start appending to it. When it comes to garbage collection, we take the page to the left and release all the page numbers it holds into the free pool and remove the page from the tree. 

Time series data: see *"Writing a Time Series Database from Scratch"* and other references. The conventional approach is to have a cache of the most recent data in memory (the vast majority of queries are for recent data), the data is highly compressed (for example Gorilla's approach can compress a tuple of a timestamp and a float from 16 bytes to under 2 bytes). When data arrives to be stored, it is chunked into a fat write that is appended to a log and then added to the cache. Periodically, the cached data is snapshotted to create historic data and a new log file is created. A snapshot is stored in a file and reflects data for a time period; it will have its own internal index for fast queries. Once created, a snapshot is immutable. The time series database is then a series of snapshot files, an in-memory cache that is persisted with an append log. If a query needs data from a snapshot then it can be mmapped. Garbage collection is handled by deletion of old snapshots. InfluxDB and Prometheus roughly follow this pattern, Gorilla and Monarch also follow this pattern but support replication and use storage such as HBase to store snapshots. Gorilla and Monarch replication is not strongly consistent, as the data is only monitoring data. 

BTrDB takes a different approach to implementing a time series database, it uses a COW tree using timestamps as the index and metrics data stored at the leaves. Internal nodes are augmented with summary information of their leaf pages to improve query performance.

Storing time series data in Digby could use log leaf pages. Since the index, time, is increasing, new data to be added to the tree would be to the rightmost pages. Two of these pages could be open as log pages; these would be the two rightmost pages in the tree. In general, most data would arrive for the rightmost page, but due to out-of-order arrival of data some data might belong to the previous page which is why we keep two open. When the log page fills, a new one is opened and one of the existing ones is closed. When a page is closed, it will be compacted and an index built for the page to allow fast indexing into the page. This means the tree will have two types of pages: the compacted data file with index (i.e., similar to an SSTable in LSM) and two log pages, the log pages being the two rightmost leaf pages. Whenever pages are created and compacted the tree's COW is used to protect against corruption or lost data. Mapping to a conventional time series database, the leaf pages map to either the snapshot file or the log file; the leaf pages could be up to 128MB in size.

Supporting a log. To implement Paxos, it is useful to have an efficient log (for example, to store the agreed input for the replicated state machine); implementing ARIES requires an efficient log as well. The approach would be to use log leaves, with the tree indexed with monotonically increasing LSN (Log Sequence Numbers). 

Supporting log leaves raises a number of challenges. If a file is opened in a filesystem to be used as a log (for example opening a file and truncating it to give it a fixed size), the filesystem can be relied on to zero the pages - this would have to be done in Digby. One challenge is detecting lost writes and redirected writes. If the write is appended to the log then it can be tricky to detect when it is lost; there must be some record either explicit or implicit that the write happened - in Digby a page pointer contains a write version. 


### Fast Paxos & Flexible Paxos
Integrating Paxos into the database could provide an interesting alternative to a traditional WAL. For instance, if Paxos outputs a queue of agreed work, this could serve as the transaction log or an intent log. Fast Paxos can reach agreement in a single round but suffers under high contention, requiring larger quorums. Flexible Paxos helps mitigate phase 2 quorum bottlenecks.

In a sharded architecture, Digby could use thousands of Paxos state machines for replication by partitioning the key namespace using a hash function. For example, 2,000 state machines could map to 2,000 independent B+ trees rooted in a single file, each utilizing COW. This could heavily leverage the parallel I/O capabilities of NVMe drives, with cross-shard transactions utilizing Paxos Commit. A forest of trees would mean losing range queries.

## Future Explorations

*   **Top-Down Tree Updates**: Digby currently uses a bottom-up approach to writing nodes (leaf -> directory -> master page). This is sub-optimal compared to the top-down approach advocated in *"B-trees, Shadowing, and Clones,"* which writes directory pages on the way down, using preemptive splits to allow better parallelism.
*   **Per-Table Page Sizes**: Allow different B+ trees (tables) within the same database to configure distinct directory and leaf page sizes.
*   **MVCC (Multi-Version Concurrency Control)**: Extend the existing rudimentary versioning system to support more complex concurrent transactions.
*   **Performance Optimizations**:
    *   **Page Cache**: Enhance the rudimentary page cache and reduce unnecessary page copying when dealing with encrypted pages.
    *   **Asynchronous I/O**: Investigate `io_uring` (via Rust's `tokio`) for async I/O, and investigate support for direct I/O. Currently, new pages overwrite existing free pages synchronously, followed by a double `sync_data` around the master page write. `io_uring` could schedule page write-backs and `sync_file_range` asynchronously, waiting on the batch before writing the master page.
    *   **Log-Structured Leaves**: Explore update optimizations similar to Bcachefs (e.g., logging changes into 256K leaf page chunks and compacting/splitting them when full) to reduce the write amplification of standard COW B-trees.
*   **Concurrency**: Add support for multi-threaded access. The current COW design supports a single writer and multiple readers. Moving to top-down tree writing would be the first step toward better concurrent writer scaling.
*   **Untorn Writes**: Investigate leveraging Linux untorn writes (atomic writes of multiple aligned blocks, like 16K on NVMe SSDs). This avoids the double-write penalty of traditional WALs. MySQL saw performance degradation with 16K untorn writes due to write amplification on its 512-byte log blocks, so integrating this effectively into Digby requires careful design.
*   **Direct NVMe Access**: Explore bypassing the filesystem to access NVMe as a raw KV store for Digby blocks (e.g., referencing *"SAKER: A Software Accelerated Key-value Service via the NVMe Interface"*).
*   **Support for Clones or More Snapshots**: Add support for tree clones following *"B-trees, Shadowing, and Clones"*.

## References

*   *"B-trees, Shadowing, and Clones"*
*   *"Deletion Without Rebalancing in Multiway Search Trees"*
*   *"Parity Lost and Parity Regained"*
*   *"B-trees Are Back: Engineering Fast and Pageable Node Layouts"*
*   *"An Evaluation of B-tree Compression Techniques"*
*   *"Efficient Free Space Reclamation in WAFL"*
*   *"File System Design for an NFS File Server Appliance"*
*   *"BTRFS: The Linux B-tree Filesystem"*
*   *"SAKER: A Software Accelerated Key-value Service via the NVMe Interface"*
*   *"ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"*
*   *"Repeating History Beyond ARIES"*
*   *"The Zettabyte File System"*
*   *"Paxos made simple"*
*   *"Consensus on Transaction Commit"*
*   *"Fast Paxos"*
*   *"Flexible Paxos: Quorum intersection revisited"*
*   *"Relaxing Quorum Intersection for Fast Paxos"*
*   [Btrfs extent reference counting](https://josefbacik.github.io/kernel/btrfs/2021/12/16/btrfs-extent-reference-counting.html)
*   [How ZFS Snapshots Really Work](https://papers.freebsd.org/2019/bsdcan/ahrens-how_zfs_snapshots_really_work/)
*   [bcachefs Snapshots](https://bcachefs.org/Snapshots/)
*   *"Gorilla: A Fast, Scalable, In-Memory Time Series Database"*
*   *"BTrDB: Optimizing Storage System Design for Timeseries Processing"*
*   *"Monarch: Google's Planet-Scale In-Memory Time Series Database"*
*   *"Writing a Time Series Database from Scratch"* [link](https://web.archive.org/web/20180125094157/https://fabxc.org/tsdb/)
*   *"InfluxDB’s TSM (Time-Structured Merge Tree) Design Docs"*

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

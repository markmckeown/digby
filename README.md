# Digby: A Rust-based Key-Value Store

Digby is an embedded key-value store written in Rust, built as a project to learn Rust and explore database implementation concepts. It uses a B+ Tree as its core data structure.

Anything with a '(?)' indicates this is not confirmed.

## Features

*   **B+ Tree Based**: Uses a B+ tree for storing key-value pairs.
*   **Global & Table-based Stores**: Supports a root global B+ Tree as well as independent B+ trees (tables), all stored in a single file.
*   **Large Item Support**: Large keys and values can be stored with 64 bit sizes. They are stored using overflow pages and they can be compressed with LZ4 (similar to TOAST in Postgres except everything is stored in the same file). Large keys are indexed using a combination of their prefix and a SHA256 hash. The first 223 bytes of the key are used as a prefix plus 32 bytes for SHA256; this allows lexical sorting up to 223 bytes of key.
*   **Multi Block and Page Sizes**: The base block size can be configured. Pages are made up of one or more blocks. The block size should be a size that the OS can atomically write, for example 4K in general on Linux (though a larger size could be used if using untorn writes in Linux). The size of key metadata pages (root page, master pages and free directory pages) is fixed at a single block. The size option for pages follows a power of two (4K, 8K, 16K..1024K) pattern. Tree directory pages and leaf page sizes can be set independently, eg one block for tree directory pages and 2 blocks for leaf pages. Overflow pages use the minimum number of blocks to store a tuple similar to ZFS, with compressed tuples up to 1MB stored in a single block (this approach optimises the number of blocks used but leads to wasted space). Blocks are managed by a slab allocator.  
*   **Copy-On-Write (COW)**: Based on "B-trees, Shadowing, and Clones" paper, similar to ZFS and BcacheFS filesystems and mdb database (?).
*   **Deletion**: Implements deletion without requiring complex tree rebalancing, based on "Deletion Without Rebalancing in Multiway Search Trees" paper.
*   **Data Integrity and Security**:
    *   Uses xxhash32 checksums for page integrity verification.
    *   Optional AES-128-GCM encryption for all stored content, which includes its own integrity checks.
*   **Compression**: Optional lz4 compression for large keys and values.
*   **Large Store Support**: Page numbers are 64 bits to support very large databases, effective addressing is 56 bits as 8 bits are used to encode the page size.
*   **Head and Tail Compression**: Head and tail compression in B+ tree nodes based on `https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf`
*   **Transactions**: Support for transactions to make multiple changes to the DB in an atomic operation which is isolated from readers. All updates are durable, the D in ACID; changes are sync'd to the disk before returning control to the client. Currently only supports a single writer at a time. 

## Usage

To use `digby` in your project, add it to your `Cargo.toml`:

```toml
[dependencies]
digby = "0.2" # Replace with the desired version
```

### Example

Here is a simple example of how to create a database, put a value, and then retrieve it.

```rust
use digby::{Db, CompressorType};
use std::fs;

fn main() {
    let db_path = "my_database.db";
    
    // Create or open the database.
    // The second argument is an optional key for encryption.
    let mut db = Db::new(db_path, None, CompressorType::None);

    let key = b"hello";
    let value = b"world";

    // Put a key-value pair into the database.
    db.put(key, value);

    // Get the value back.
    if let Some(retrieved_value) = db.get(key) {
        println!("Retrieved value: {}", String::from_utf8_lossy(&retrieved_value));
        assert_eq!(retrieved_value, value);
    } else {
        println!("Value not found!");
    }

    // Clean up the database file.
    fs::remove_file(db_path).expect("Failed to remove database file");
}
```

## Transactions, COW vs ARIES

COW is used in ZFS and BcacheFS filesystems and also in the mdb database. In older
literature the approach is known as "page shadowing", eg System R. The alternate
approach is ARIES, a WAFL with redo and undo phases for recovery. I *think* COW
works for filesystems as they do not need to support complex transactions, while
for mdb there is a single writer only and it is designed for high read volume and
low write volume for LDAP. 

Within digby transactions are supported via the "_txn" version of the methods;
the client starts a transaction with db.new_transaction and is provided with a 
transaction context that it passes to any subsequent methods in the transaction. 
When ready to commit, the client calls db.commit with the transaction context. 
Each operation that is part of the transaction makes the changes to the db 
tree but does not update the master page; that is done in the commit. 
Currently digby does not have any thread protection, but as this approach is using COW
it means you can have multiple readers that do not block each other or the writer,
but you can only have a single writer (RCU). The readers can use the version information
in the pages and/or tuples to determine if the version of the tree they are using is
no longer valid and retry.

To support complex transactions with multiple writers and rollback requires a 
variation of ARIES? If switching to an ARIES type approach can a log be done 
in the same file as the tree? 

## Checksums and Merkle Trees
Both ZFS and BcacheFS store the checksum for a page in the pointer to the page/object; 
the checksum is not stored in the page/object. The exception is the root of the tree
which stores its own checksum. This forms a Merkle tree; Git is another example of a 
Merkle tree. This will catch more errors than simple bit rot (Phantom writes, 
Misdirected reads and writes, DMA parity errors, etc). Is part of
the reason for doing this in a filesystem that for leaf pages that hold 
user data you do not want to store a checksum in the object, it should just be
user file data?

In digby it would be possible to add the checksum to the page pointer (internally
called the page number). However, there are a number of challenges. A fixed
size for the checksum would need to be chosen - currently a 32 bit xxhash32 is used
and stored in the block; this could be switched to 64 bit xxhash3. digby also supports
encryption using AES128-GCM; this has a built in cryptographic hash which requires
96 bits to store the nonce - digby relies on built in checksum in AES128-GCM rather
than duplicating the work by adding another checksum. So embedding the checksum in 
the page pointer loses some flexibility. The page number is also in the digby page,
so does this protect against phantom writes, misdirected reads and writes etc?
Another disadvantage of storing the checksum in the page pointer is that more room is
used in the internal directory nodes.

So to support Merkle tree in digby would need to pick a checksum with uncontroversial
size, eg 64 bits such as for xxhash64 or xxhash3. If encryption was also used 
then pay the price of double checksumming. The page pointer would then be 128 bits,
64 bits for checksum, maybe 8 bits for encoding the page/block size (calculated as 
4096 << size) and leaving 56 bits for addressing. Could also encode the page type 
in the page pointer in 8 bits leaving 48 bits for addressing (ie 1EiB).

## Fast Paxos & Flexible Paxos

An interesting challenge would be to integrate Paxos into the database. For example 
Paxos outputs a queue of agreed work to execute; could this be the WAFL for the database?
Fast Paxos can make agreements in a single round of communication, however it suffers
when there is a lot of contention and requires more than a simple majority to proceed.
Flexible Paxos helps address some of the limitations of Fast Paxos, i.e., the number of nodes
needed to reach consensus in phase 2. In Fast Paxos contention can 
be addressed by each node having an agreed approach to conflicts - given a set of conflicting
work items the nodes independently resolve them the same way leading to a consistent 
outcome across all nodes.

Further to this the database could be replicated using thousands of Paxos state machines
by sharding the key namespace. For example could there be 2K state machines, with 2K trees 
rooted in the one file each using COW. Would this open up the parallel nature of NVMe drives? 
Transactions across state machines could use Paxos Commit. Is the advantage of a B+
tree of naturally supporting ranges lost?

See "Relaxing Quorum Intersection for Fast Paxos". 

## Future Things to Explore

Future plans include:

*   **Root Page Changes**: Add block size, tree leaf and dir page size to root page. Read/write root page outside page cache, ie directly via FileLayer - assert on attempts to read/write to root page (ie NPE) in page cache. Do not encrypt root page. Speculatively read bytes from start of file to get root page contents to determine block size and then read root page using block size. 
*   **Add xxhash64 checksum**: With support for 1MB pages add support for stronger checksum.
*   **Allow different tables to have different page sizes**: Tree directory and leaf page sizes can be configured globally, allow different trees to support different directory and leaf page sizes. 
*   **MVCC (Multi-Version Concurrency Control)**: Extend existing simple versioning system. This is tied to supporting more complex transactions than is currently supported.
*   **Performance Optimizations**:
    *   Implement a proper page cache.
    *   Investigate `io_uring` for async I/O. Current approach is that as the tree is being changed the new pages are written out, overwriting existing free pages. Once all the tree pages are written out including the new tree root, sync data is called to make sure the pages are on disk, the master page is written out and then sync data is called again. Using `io_uring` rather than waiting for the pages to be written out they can be scheduled for write back using `io_uring` - it may be possible to chain the write and `sync_file_range` in `io_uring`. Then when coming to write and sync the master page wait until `io_uring` has done all its tasks before writing and syncing the master page. This should be done using Rust Tokio.   
    *   Explore update optimizations similar to Bcachefs. Bcachefs uses a COW approach were the path through the b+ tree is updated on a change which means for an update in a leaf page multiple pages are written out. It developed an optimisation where part of a leaf pages was used a log for the leaf page, for example if the page was 64K then 16K was devoted to being a log for the leaf node. If a value was updated or added to the leaf then it would be added to the log with a flag indicating that it was added, similarly if a value was deleted - the key was added to the log with a delete flag. When reading the leaf page the log was checked first before accessing the leaf data itself. Once the log was full the leaf page was rewritten with an empty log, or possibly split. An advantage with this approach is that updates only require a single page write in general, if the leaf page has to be re-written then the whole path in the tree is re-written per COW.
*   **Concurrency**: Add support for multi-threaded access. Current support for transactions in digby and the COW design means that it can support a single writer with multiple readers, the readers would not block the writer or each other and the writers should not block the readers. Readers can use versions in pages/tuples to determine if they are on a stale version of the tree. First step would be to switch to top down writing of the tree rather than bottom up.
*   **Untorn Writes**: Investigate using Linux untorn writes. Linux has added support for untorn writes. Before this the kernel would write a limited amount of bytes as an atomic action, this was generally 4K (or the page size). Untorn writes were added to support database use cases, to avoid double writing data in a log and then into the database. Untorn writes allow writes of multiple page sizes as an atomic action, for example 16K can either be written or not. The bytes must be aligned. This seems to align with nvme SSD that do writes in 16K block(?). Interestingly when untorn writes were tested with MySQL with 16K pages performance degraded, MySQL writes in 512 byte blocks to a log file - with 4K pages this is a 8x amplification and with 16K pages it is a 32x amplification. How can untorn writes be used in digby?
*   **Code Quality**: Improve the Rust implementation.
*   **Fast Flexible Paxos**: For replication.
*   **Unnecessary copying in page cache.**: Unnecessary page copying in page cache to deal with encrypted pages.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

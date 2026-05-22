# Digby: A Rust-based Key-Value Store

Digby is an embedded key-value store written in Rust, built as a learning project to learn Rust and explore database implementation concepts. It uses a B+ Tree as its core data structure.

## Features

*   **B+ Tree Based**: Use a B+ tree for storing  key-value pairs.
*   **Global & Table-based Stores**: Supports a root global B+ Tree as well as independent B+ trees (tables), all stored in a single file.
*   **Large Item Support**: Large keys and values can be stored with 64 bit sizes. They are stored using overflow pages and they can be compressed with LZ4. Large keys are indexed using a combination of their prefix and a SHA256 hash. The first 224 bytes of the key is used as a prefix plus 32 bytes for SHA256, this allows lexical sorting up to 224 bytes of key.
*   **Copy-On-Write (COW)**: Based on "B-trees, Shadowing, and Clones" paper, similar to ZFS and BcacheFS.
*   **Deletion**: Implements deletion without requiring complex tree rebalancing, based on "Deletion Without Rebalancing in Multiway Search Trees" paper.
*   **Data Integrity and Security**:
    *   Uses xxhash32 checksums for page integrity verification.
    *   Optional AES-128-GCM encryption for all stored content, which includes its own integrity checks.
*   **Configurable**: Block/page size is configurable.
*   **Compression**: Optional lz4 compression for large keys and values.
*   **Large Store Support**: Page numbers are 64 bits to support very large databases.
*   **Head and Tail Compression**: Head and tail compression in B+ tree nodes based on https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf

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

## COW vs ARIES

COW is used in ZFS and BcacheFS filesystems and also mdb database. In older
literature the approach is known as "page shadowing", eg System R. The alternate
approach is ARIES, a WAFL with redo and undo phases for recovery. I *think* COW
works for filesystems as they do not need to support complex transactions, while
for mdb there is a single writer only and it is designed for high read volume and
low write volume for LDAP. 

To support complex transactions will a variation of ARIES need to be used? Can
we do the log in the same file?

## Checksums and Merkle Trees
Both ZFS and BcacheFS store the checksum for a page in the pointer to the page/object, 
the checksum is not stored in the page/object. The exception is the root of the tree
which stores its own checksum. This forms a Merkle tree, Git is another example of a 
Merkle tree. This will catch more errors than simple bit rot (Phantom writes, 
Misdirected reads and writes, DMA parity errors, etc). Is part of
the reason for doing this in a filesystem is that for leaf pages that hold 
user data you do not want to store a checksum in the object, if should just be
user file data?

In digby it would be possible to add the checksum to the page pointer (internally
called the page number). However, there are a number of challenges. A fixed
size for the checksum would need to be chosen - currently a 32 bit xxhash32 is used
and stored in the block, this could be switched to 64 bit xxhash. digby also supports
encryption using AES128-GCM, this has a built in cyrpographic hash which requires
96 bits to store the nonce - digby relies on built in checksum in AES128-GCM rather
than duplicating the work by adding another checksum. So embedding the checksum in 
the page pointer loses some flexibility. The page number is also in the digby page,
so does this protect against phantom writes, misdirected reads and writes etc?
Anther disadvantage of storing the checkum in the page pointer is that more room is
used in the internal directory nodes.

So to support Merkle tree in digby would need to pick a checksum with uncontroversal
size, eg 64 bits with for xxhash64 or xxhash3. If encryption was also used 
then pay the price of double checksuming. The page pointer would then be 128 bits,
64 bits for checksum, maybe 8 bits for encoding the page/block size (calculated as 
4096 << size) and leaving 56 bits for addressing. Could also encode the page type 
in the page pointer in 8 bits leaving 48 bits for addressing (ie 1EiB).


## Future Things to Explore

Future plans include:

*   **Multiple Page Sizes** Embed the page/block size in the page number, eg use first byte in the page number as a multiple of 4K. This would allow different sizes for internal nodes (directory nodes) and for leaf pages. Overflow pages could be very large, eg up to 2 MB. The page sizes supported could be 4K, 8K, 16K, 32K, 64K, 128K, 256K, 512K, 1024K and 2048K - there could be a free page allocator for each size.
*   **MVCC (Multi-Version Concurrency Control)**: Extend existing simple versioning system.
*   **Performance Optimizations**:
    *   Implement a proper page cache.
    *   Investigate `io_uring` for async I/O.
    *   Explore update optimizations similar to Bcachefs (e.g., using LSM Tree concepts). Use large leaf pages that have built in log.
    *   Use some of the 64 bit page number for caching, eg "pointer swizzling".
*   **Concurrency**: Add support for multi-threaded access.
*   **Filesystem Integration**: Investigate using Linux untorn writes. 
*   **Code Quality**: Improve the Rust implementation.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

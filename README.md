# Digby: A Rust-based Key-Value Store

Digby is an embedded key-value store written in Rust, built as a learning project to learn Rust and explore database implementation concepts. It uses a B+ Tree as its core data structure.

## Features

*   **B+ Tree Based**: Efficient key-value storage and retrieval.
*   **Global & Table-based Stores**: Supports a single global B+ Tree as well as independent trees (tables).
*   **Large Item Support**: Keys and values can be up to 4GB, handled via overflow pages. Large keys are indexed using a combination of their prefix and a SHA256 hash.
*   **Copy-On-Write (COW)**: For data integrity. Based on "B-trees, Shadowing, and Clones" but implementation is actually bottom up rather than top down and uses a stack rather than recursion.
*   **Safe Deletion**: Implements deletion without requiring complex tree rebalancing, based on "Deletion Without Rebalancing in Multiway Search Trees".
*   **Data Integrity and Security**:
    *   Uses xxhash32 checksums for page integrity verification.
    *   Optional AES-128-GCM encryption for all stored content, which includes its own integrity checks.
*   **Configurable**: Block/page size is configurable.
*   **Compression**: Optional lz4 compression for large keys and values.
*   **Large Store Support**: Page numbers are 64 bits to support very large databases.

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

## Roadmap and Future Work

This project is under active development. Future plans include:

*   **MVCC (Multi-Version Concurrency Control)**: Extend existing simple versioning system.
*   **Performance Optimizations**:
    *   Implement a proper page cache.
    *   Add support for tail/head compression in B+ Tree pages based on https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf
    *   Rewrite internal and leaf page implementations for better efficiency, move memory around in the page rather than rewriting the while page.
    *   Investigate `io_uring` for async I/O.
    *   Explore update optimizations similar to Bcachefs (e.g., using LSM Tree concepts). Use large leaf pages that have built in log.
    *   Use some of the 64 bit page number for caching, eg "pointer swizzling".
*   **Concurrency**: Add support for multi-threaded access.
*   **Filesystem Integration**: Investigate using Linux untorn writes. 
*   **Code Quality**: Improve the Rust implementation.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

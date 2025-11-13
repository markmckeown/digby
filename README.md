# digby
Rust KV Store Using a B+ Tree

Version: 0.1

## Goal - Learn Rust by implementing a KV store.

## Features:
* B+ Tree for storing key-value pairs.
* Keys and Values can be up to 4GB in size. This uses overflow pages, large keys are stored using their first 224 bytes along with 32 bytes of the key's SHA256 in the B+ tree - whole key and value is stored compressed in overflow pages.
* Uses Copy-On-Write (COW) for storing data safely. Based on "B-trees, Shadowing, and Clones" but current implementation is actually bottom up rather than top down and uses a stack rather than recursion.
* Deletion follows the approach of "Deletion Without Rebalancing in Multiway Search Trees".
* Uses xxhash_32 checksum for each page to detect corruption, unless AES-128-GCM is used for encryption which has a built in checksum. Checksum is stored in page and not in page pointer, could change to use this approach similarly to ZFS and bcachefs. 
* Option to use AES-128-GCM to encrypt all contents.
* Block/page size is configurable, goal is to use Linux untorn writes to support blocks larger than 4K ie 16K.
* lz4 compression for large keys/values.
* Other checksum, compression and encryption approaches could be supported.
* key-value tuples have simple versions, plan is to extend to MVCC.

## TODO
* Rust code is very crude and NOOB level.
* Add support for tables, along side a a global Tree. Global tree should be simple and fast, tables should support cross table transactions.
* page_cache does not actually cache pages.
* support multiple page/block sizes, help with large values. Use a slab allocator approach similar to ZFS to do this.
* Add support for tail/head compression  interal/leaf pages per https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf Re-write internal and leaf page implementations which are totally inefficient per above reference..
* Investigate support for adding MVCC.
* Investigate using untorn writes in Linux.
* Investigate using io_uring for writes/reads.
* Investigae optimizing COW updates similar to Bcachefs. Use a log to store updates before adding to tree. In Bcachefs the tree nodes use LSM Tree mechanisms.
* Support multiple threads.
* Switch to support 64 bit page number rather than 32 bit. Provides larger capacity but could support different page/block sizes by encoding page size in the page number, 48 bits could be used for addressing leaving 16 bits for page size and other page metadata (pointer swizzling?). Optionaly could use 96 bits and store the checksum in the page pointer per ZFS and Bcachefs.



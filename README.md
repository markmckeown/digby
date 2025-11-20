# digby
Rust KV Store Using a B+ Tree

Version: 0.2

## Goal - Learn Rust by implementing a KV store.

## Features:
* B+ Tree for storing key-value pairs.
* There is a global B+ tree and also support for tables or independent B+ trees. 
* Keys and Values can be up to 4GB in size. This uses overflow pages, large keys are stored using their first 224 bytes along with 32 bytes of the key's SHA256 in the B+ tree - whole key and value is stored compressed in overflow pages.
* Uses Copy-On-Write (COW) for storing data safely. Based on "B-trees, Shadowing, and Clones" but current implementation is actually bottom up rather than top down and uses a stack rather than recursion.
* Deletion follows the approach of "Deletion Without Rebalancing in Multiway Search Trees".
* Uses xxhash_32 checksum for each page to detect corruption, unless AES-128-GCM is used for encryption which has a built in checksum. Checksum is stored in page and not in page pointer, could change to use this approach similarly to ZFS and bcachefs. 
* Option to use AES-128-GCM to encrypt all contents.
* Block/page size is configurable, goal is to use Linux untorn writes to support blocks larger than 4K ie 16K.
* lz4 compression for large keys/values.
* Other checksum, compression and encryption approaches could be supported.
* key-value tuples have simple versions, plan is to extend to MVCC.
* Page numbers are 64 bits - capacity is 2^64 * 4K (4 ZiB). However, some of the page number bits may be used for page size or "pointer swizzling" in the future which will reduce capacit (2^40 * 4K is 4PiB). The extra capacity comes at a cost as the page numbers have doubled in size reducing capacity of internal tree nodes etc - in reality 32 bit page number numbers is probably good enough at 16 TiB capacity. 

## TODO
* Rust code is very crude and NOOB level.
* page_cache does not actually cache pages.
* support multiple page/block sizes, help with large values. Use a slab allocator approach similar to ZFS to do this.
* Add support for tail/head compression  interal/leaf pages per https://www.cs.purdue.edu/homes/csjgwang/pubs/SIGMOD24_BtreeCompression.pdf Re-write internal and leaf page implementations which are totally inefficient per above reference..
* Investigate support for adding MVCC.
* Investigate using untorn writes in Linux.
* Investigate using io_uring for writes/reads.
* Investigae optimizing COW updates similar to Bcachefs. Use a log to store updates before adding to tree. In Bcachefs the tree nodes use LSM Tree mechanisms to avoid rewriting the tree on update. For examlpe a leaf node could be 64K, 16K could be a log for the node. Updates would only update a log page in the leaf node until the node is full then the node would be compacted copied using COW.
* Support multiple threads.



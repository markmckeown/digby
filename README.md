# digby
Rust KV Store

## Goal - learn Rust by writing a KV store.

## Features:
* B+ tree for storing key-value pairs.
* Keys and Values can be up to 4GB in size. This uses overflow pages, large keys are stored using their first 224 bytes along with 32 bytes of the key's SHA256 in the B+ tree - whole key and value is stored compressed in overflow pages.
* Support for tables. Also a global B+ Tree.
* Uses Copy-On-Write (COW) for storing data safely.
* Uses xxhash_32 checksum for each page to detect corruption.
* Option to use AES-128-GCM to encrypt all contents.
* Block/page size is configurable, goal is to use Linux untorn writes to support blocks larger than 4 ie 16K.
* Optionally uses lz4 compression for large keys/values.
* Other checksum, compression and encryption approaches can be supported.
* key-value tuples have versions to support MVCC.

## TODO
* Cannot delete keys at db layer - supported in leaf pages.
* page_cache does not actually cache pages.
* Support adding tables and put/get/delete on their trees.
* Add support for key prefix delta compression in directory and leaf pages.
* Code to manipulate pages contents is very inefficient - whole page is re-written on change. Should use memmove/memcopy Rust equivalents.
* Should each key/value have a version to support MVCC, or should it inherit the version from its page.
* Investigate using untorn writes in Linux.
* Investigate using io_uring for writes/reads.
* Investigae optimizing COW updates, see bcachefs use of log and extents.
* Support multiple threads.



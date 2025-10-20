# digby
Rust KV Store

Goal - learn Rust by writing a KV store in Rust.

Features:
B+ tree for storing key-value pairs.

Keys and Values can be up to 4GB in size. This uses overflow pages, large keys are stored using their forst 224 bytes along
with 32 bytes of the key's SHA256 in the B+ tree.

Support for Tables, with a global B+ Tree.

Uses Copy-On-Write (COW) for storing data safely.

Uses xxhash_32 checksum for each page to detect corruption.

Option to use AES-128-GCM to encrypt the store contents.

Block/page size is configurable, goal is to use Linux untorn writes to support blocks large than 4K, ie 16K.

Optionally uses lz4 compression for large keys/values.

Other checksum, compression and encryption approaches can be added later.
key-value tuples have versions to support MVCC.

TODO

Cannot delete keys at db layer - though supported in leaf pages.

page_cache does not actually cache pages.

Support adding tables and operating on their B trees.

Add support for key prefix delta compression in directory and leaf pages.

Code to manipulate pages contents is very inefficient, should use memmove/memcopy Rust equivalents - todo when
page structure stabilises.

Should each key/value have a version to support MVCC or should it inherit the version from its page.

Investigate using untorn writes in Linux.

Use io_uring for writes/reads.



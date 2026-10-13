use digby::Db;
use digby::compressor::CompressorType;
use digby::db_config::DbConfig;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn create_db(db_config: &DbConfig) -> (TempDir, String, Db) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let db = Db::create(&path, None, None, db_config).unwrap();
    (dir, path, db)
}

fn no_compression() -> DbConfig {
    DbConfig::builder()
        .compressor_type(CompressorType::None)
        .build()
}

fn missing_keys(db: &mut Db, keys: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
    keys.iter()
        .filter(|(k, v)| db.get(k).as_ref() != Some(v))
        .map(|(k, _)| String::from_utf8_lossy(k).to_string())
        .collect()
}

// A leaf split where the new tuple belongs in the left half but does not
// fit requires a second split. The pages were returned out of order, so the
// directory lost the separator for the right page and keys went missing.
#[test]
fn test_three_way_leaf_split_root() {
    let (_dir, _path, mut db) = create_db(&no_compression());
    let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for k in [b"a", b"b", b"c"] {
        keys.push((k.to_vec(), vec![7u8; 1000]));
    }
    for i in 0..60u32 {
        keys.push((format!("d{:02}", i).into_bytes(), vec![1u8]));
    }
    for (k, v) in &keys {
        db.put(k, v);
    }
    keys.push((b"b5".to_vec(), vec![9u8; 1000]));
    db.put(b"b5", &[9u8; 1000]);

    let missing = missing_keys(&mut db, &keys);
    assert!(missing.is_empty(), "missing: {:?}", missing);
}

// Same as above but the split leaf is below a directory root.
#[test]
fn test_three_way_leaf_split_non_root() {
    let (_dir, _path, mut db) = create_db(&no_compression());
    let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..400u32 {
        keys.push((format!("A{:04}", i).into_bytes(), vec![1u8; 20]));
    }
    for k in [b"a", b"b", b"c"] {
        keys.push((k.to_vec(), vec![7u8; 1000]));
    }
    for i in 0..60u32 {
        keys.push((format!("d{:02}", i).into_bytes(), vec![1u8]));
    }
    for (k, v) in &keys {
        db.put(k, v);
    }
    keys.push((b"b5".to_vec(), vec![9u8; 1000]));
    db.put(b"b5", &[9u8; 1000]);

    let missing = missing_keys(&mut db, &keys);
    assert!(missing.is_empty(), "missing: {:?}", missing);
}

// Splitting a leaf with a single entry always put the new tuple in the right
// page, even when its key was smaller than the existing key.
#[test]
fn test_single_entry_leaf_split_small_block() {
    let db_config = DbConfig::builder()
        .compressor_type(CompressorType::None)
        .block_size(2048)
        .build();
    let (_dir, _path, mut db) = create_db(&db_config);
    db.put(b"m", &[1u8; 1000]);
    db.put(b"c", &[2u8; 1000]);
    assert_eq!(db.get(b"m"), Some(vec![1u8; 1000]));
    assert_eq!(db.get(b"c"), Some(vec![2u8; 1000]));
}

// Deleting without rebalancing can leave a directory page with no
// separator keys, inserting through it then panicked.
#[test]
fn test_insert_through_empty_dir_page() {
    let (_dir, _path, mut db) = create_db(&no_compression());
    let n = 150u32;
    for i in 0..n {
        db.put(format!("k{:04}", i).as_bytes(), &[1u8; 20]);
    }
    for i in (20..n).rev() {
        db.delete(format!("k{:04}", i).as_bytes());
    }
    db.put(b"k0000", &[2u8; 20]);
    assert_eq!(db.get(b"k0000"), Some(vec![2u8; 20]));
    for i in 1..20u32 {
        assert_eq!(db.get(format!("k{:04}", i).as_bytes()), Some(vec![1u8; 20]));
    }
}

// Multi-block leaf/dir pages: a single block free dir page was returned to
// the multi-block free list and later handed out as a multi-block page.
#[test]
fn test_multi_block_pages_many_commits() {
    let db_config = DbConfig::builder()
        .leaf_pg_blk_cnt_shift(1)
        .dir_pg_blk_cnt_shift(1)
        .build();
    let (_dir, _path, mut db) = create_db(&db_config);
    for i in 0..2000u32 {
        db.put(format!("key{:08}", i).as_bytes(), &[7u8; 1000]);
    }
    for i in 0..2000u32 {
        assert_eq!(
            db.get(format!("key{:08}", i).as_bytes()),
            Some(vec![7u8; 1000])
        );
    }
}

// Multi-block leaf/dir pages: a commit freeing many multi-block pages needs
// extra free dir pages, these were created without the FreeDir page type.
#[test]
fn test_multi_block_pages_large_transaction() {
    let db_config = DbConfig::builder()
        .leaf_pg_blk_cnt_shift(1)
        .dir_pg_blk_cnt_shift(1)
        .build();
    let (_dir, _path, mut db) = create_db(&db_config);
    for value in [7u8, 8u8] {
        let mut tx_ctx = db.new_transaction();
        for i in 0..4000u32 {
            db.put_txn(
                format!("key{:08}", i).as_bytes(),
                &[value; 1000],
                &mut tx_ctx,
            );
        }
        db.commit(tx_ctx);
    }
    for i in (0..4000u32).step_by(7) {
        assert_eq!(
            db.get(format!("key{:08}", i).as_bytes()),
            Some(vec![8u8; 1000])
        );
    }
}

// A corrupt master page must not stop the database opening, the other
// master page should be used instead.
#[test]
fn test_open_with_corrupt_master() {
    use std::os::unix::fs::FileExt;
    let (dir, path, mut db) = create_db(&no_compression());
    db.put(b"a", b"1");
    db.put(b"b", b"2");
    drop(db);
    let backup = dir.path().join("backup");
    std::fs::copy(&path, &backup).unwrap();

    // Master pages are in blocks 1 and 2. Corrupting the current master
    // falls back to the previous one which does not contain "b".
    let mut b_found = 0;
    for slot in [1u64, 2u64] {
        std::fs::copy(&backup, &path).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0xAB; 100], 4096 * slot + 2000).unwrap();
        drop(file);

        let mut db = Db::open(&path, None, None).unwrap();
        assert_eq!(db.get(b"a"), Some(b"1".to_vec()), "corrupt slot {}", slot);
        if db.get(b"b") == Some(b"2".to_vec()) {
            b_found += 1;
        }
    }
    assert_eq!(b_found, 1);
}

// A 255 byte key equal to the stored form of a large key (223 byte prefix
// + SHA-256) must not alias the large key.
#[test]
fn test_short_key_does_not_alias_large_key() {
    let (_dir, _path, mut db) = create_db(&no_compression());
    let big = vec![b'q'; 400];
    let mut short = big[..223].to_vec();
    short.extend_from_slice(&Sha256::digest(&big));
    assert_eq!(short.len(), 255);
    db.put(&big, b"big");
    db.put(&short, b"short");
    assert_eq!(db.get(&big), Some(b"big".to_vec()));
    assert_eq!(db.get(&short), Some(b"short".to_vec()));
}

// As above but with a deeper tree, so directory pages above the leaf
// parents are also left without separator keys. Long keys keep the
// directory fan out small so the tree is deep with few keys.
#[test]
fn test_insert_through_empty_dir_pages_deep_tree() {
    let (_dir, _path, mut db) = create_db(&no_compression());
    let key = |i: u32| format!("k{:06}{}", i, "x".repeat(200)).into_bytes();
    let n = 600u32;
    let value = [1u8; 1000];
    for i in 0..n {
        db.put(&key(i), &value);
    }
    for i in (100..n).rev() {
        assert!(db.delete(&key(i)));
    }
    for i in (100..n).step_by(13) {
        db.put(&key(i), &[2u8; 1000]);
    }
    for i in 0..100u32 {
        assert_eq!(db.get(&key(i)), Some(value.to_vec()));
    }
    for i in 100..n {
        let expected = if (i - 100) % 13 == 0 {
            Some(vec![2u8; 1000])
        } else {
            None
        };
        assert_eq!(db.get(&key(i)), expected, "i {}", i);
    }
}

fn append_bytes(path: &str, len: usize) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(&vec![0x5Au8; len]).unwrap();
}

fn create_mirrored_db(dir: &TempDir) -> (String, String) {
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let mirror = dir.path().join("mirror").to_str().unwrap().to_string();
    let mut db = Db::create(&path, Some(&mirror), None, &no_compression()).unwrap();
    db.put(b"a", b"1");
    (path, mirror)
}

// A crash while extending the files can leave uncommitted blocks at the end
// of the primary and mirror, with different lengths and a partial block.
// Growing the files afterwards panicked as the lengths did not match.
#[test]
fn test_open_after_crash_extending_files() {
    let dir = TempDir::new().unwrap();
    let (path, mirror) = create_mirrored_db(&dir);
    append_bytes(&mirror, 3 * 4096);
    append_bytes(&path, 4096 + 100);

    let mut db = Db::open(&path, Some(&mirror), None).unwrap();
    for i in 0..200u32 {
        db.put(format!("k{:04}", i).as_bytes(), &[1u8; 1000]);
    }
    drop(db);

    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        std::fs::metadata(&mirror).unwrap().len()
    );
    let mut db = Db::open(&path, Some(&mirror), None).unwrap();
    assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
    for i in 0..200u32 {
        assert_eq!(
            db.get(format!("k{:04}", i).as_bytes()),
            Some(vec![1u8; 1000])
        );
    }
}

// A partial block at the end of a database without a mirror.
#[test]
fn test_open_with_partial_block() {
    let (_dir, path, mut db) = create_db(&no_compression());
    db.put(b"a", b"1");
    drop(db);
    append_bytes(&path, 100);

    let mut db = Db::open(&path, None, None).unwrap();
    for i in 0..200u32 {
        db.put(format!("k{:04}", i).as_bytes(), &[1u8; 1000]);
    }
    assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(db.get(b"k0199"), Some(vec![1u8; 1000]));
}

// Opening a mirrored database without its mirror and committing leaves the
// mirror out of date, opening with it must fail rather than panic later.
#[test]
fn test_open_with_out_of_date_mirror() {
    let dir = TempDir::new().unwrap();
    let (path, mirror) = create_mirrored_db(&dir);
    let mut db = Db::open(&path, None, None).unwrap();
    for i in 0..200u32 {
        db.put(format!("k{:04}", i).as_bytes(), &[1u8; 1000]);
    }
    drop(db);

    match Db::open(&path, Some(&mirror), None) {
        Err(digby::db::DigbyError::MirrorOutOfDate(_)) => (),
        Err(e) => panic!("unexpected error {:?}", e),
        Ok(_) => panic!("open should fail with an out of date mirror"),
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8 ^ seed).collect()
}

fn check_large_values(db_config: &DbConfig, key: Option<Vec<u8>>) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let mirror = dir.path().join("mirror").to_str().unwrap().to_string();
    // Sizes needing one block, a multi-block page, a page over 64K and a
    // chain of maximum size pages.
    let sizes = [2_000usize, 20_000, 100_000, 3_000_000];
    {
        let mut db = Db::create(&path, Some(&mirror), key.clone(), db_config).unwrap();
        for (i, size) in sizes.iter().enumerate() {
            db.put(format!("k{}", i).as_bytes(), &pattern(*size, 1));
        }
        // Large key as well as large value.
        db.put(&vec![b'K'; 1000], &pattern(200_000, 2));
    }
    let mut db = Db::open(&path, Some(&mirror), key).unwrap();
    for (i, size) in sizes.iter().enumerate() {
        assert_eq!(
            db.get(format!("k{}", i).as_bytes()),
            Some(pattern(*size, 1)),
            "size {}",
            size
        );
    }
    assert_eq!(db.get(&vec![b'K'; 1000]), Some(pattern(200_000, 2)));

    // Overwriting and deleting frees the overflow pages for reuse, the file
    // should not keep growing.
    let file_size = std::fs::metadata(&path).unwrap().len();
    for round in 0..4u8 {
        for (i, size) in sizes.iter().enumerate() {
            db.put(format!("k{}", i).as_bytes(), &pattern(*size, round));
        }
    }
    for (i, size) in sizes.iter().enumerate() {
        assert_eq!(
            db.get(format!("k{}", i).as_bytes()),
            Some(pattern(*size, 3)),
            "size {}",
            size
        );
    }
    assert!(db.delete(b"k3"));
    assert_eq!(db.get(b"k3"), None);
    db.put(b"k3", &pattern(3_000_000, 9));
    assert_eq!(db.get(b"k3"), Some(pattern(3_000_000, 9)));
    let grown = std::fs::metadata(&path).unwrap().len();
    assert!(
        grown <= file_size * 2,
        "file grew from {} to {} bytes",
        file_size,
        grown
    );
}

// Overflow tuples are stored in pages sized to fit them, up to 1M.
#[test]
fn test_large_values_no_compression() {
    check_large_values(&no_compression(), None);
}

#[test]
fn test_large_values_lz4() {
    check_large_values(&DbConfig::builder().build(), None);
}

#[test]
fn test_large_values_encrypted() {
    let db_config = DbConfig::builder()
        .block_sanity(digby::block_sanity::BlockSanity::Aes128Gcm)
        .build();
    check_large_values(&db_config, Some(b"0123456789abcdef".to_vec()));
}

// A large value that LZ4 compresses small enough is stored in the leaf as
// ValueCompressed. Its compressed bytes were treated as an overflow page
// number when reading, overwriting or deleting it.
#[test]
fn test_compressed_value_in_leaf() {
    let (_dir, _path, mut db) = create_db(&DbConfig::builder().build());
    let value = vec![7u8; 5000];
    db.put(b"a", &value);
    assert_eq!(db.get(b"a"), Some(value.clone()));
    db.put(b"a", &[8u8; 5000]);
    assert_eq!(db.get(b"a"), Some(vec![8u8; 5000]));
    assert!(db.delete(b"a"));
    assert_eq!(db.get(b"a"), None);
}

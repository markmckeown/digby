use digby::Db;
use digby::compressor::CompressorType;
use rand::RngCore;
use rand::prelude::SliceRandom;
use rand::rng;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn test_digby_db() {
    let mut _db = Db::new("/tmp/test_db.db", None, CompressorType::None);
    let _ = std::fs::remove_file("/tmp/test_db.db");
}

#[test]
fn test_db_store_large_key_value_incompressible() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let mut key: Vec<u8> = vec![0u8; 8192];
    let mut value: Vec<u8> = vec![0u8; 18192];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut key);
    rng.fill_bytes(&mut value);
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_store_large_key_value_compressible() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key: Vec<u8> = vec![111u8; 8192];
    let value: Vec<u8> = vec![56u8; 18192];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Db compression mis-match, stored type is 1, requested type None")]
fn test_db_store_large_key_value_compressible_mismatch() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key: Vec<u8> = vec![111u8; 8192];
    let value: Vec<u8> = vec![56u8; 18192];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::LZ4,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let _db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_clear_large_tuples() {
    let size = 32u64;
    let mut large_value = vec![0u8; 5000];
    let block_size = 4096;
    rng().fill_bytes(&mut large_value);
    let mut numbers: Vec<u64> = (0..=size).collect();
    let mut rng = rng();
    numbers.shuffle(&mut rng);
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    {
        let mut db = Db::new_with_page_size(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
            block_size,
        );
        for i in &numbers {
            let mut key = vec![0u8; 512];
            key[0..8].copy_from_slice(i.to_be_bytes().as_ref());
            db.put(&key, &large_value);
        }
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new_with_page_size(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
            block_size,
        );
        numbers.shuffle(&mut rng);
        for i in &numbers {
            let mut key = vec![0u8; 512];
            key[0..8].copy_from_slice(i.to_be_bytes().as_ref());
            let returned_value = db.get(&key);
            assert!(returned_value.is_some());
            assert_eq!(large_value, returned_value.unwrap());
        }
        db.clear();
        let key = vec![0u8; 512];
        let returned_value = db.get(&key);
        assert!(returned_value.is_none());
    }
    {
        let mut db = Db::new_with_page_size(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
            block_size,
        );
        let mut numbers: Vec<u64> = (0..=size).collect();
        numbers.shuffle(&mut rng);
        for i in &numbers {
            let mut key = vec![0u8; 512];
            key[0..8].copy_from_slice(i.to_be_bytes().as_ref());
            let returned_value = db.get(&key);
            assert!(returned_value.is_none());
        }
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

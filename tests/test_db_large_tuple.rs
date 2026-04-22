use digby::Db;
use digby::compressor::CompressorType;
use std::fs;
use tempfile::NamedTempFile;
use rand::{RngCore};

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
        }
        fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}
use digby::Db;
use digby::compressor::CompressorType;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn test_db_store_value_with_encryption() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::None,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::None,
        );
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Failed to decrypt page")]
fn test_db_store_value_with_encryption_wrong_key() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::None,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(b"bad_encryption_key".to_vec()),
            CompressorType::None,
        );
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Calculated checksum does not match stored checksum for page")]
fn test_db_store_value_with_encryption_no_key() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::None,
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
fn test_db_store_large_key_value_compressible_encryption() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key: Vec<u8> = vec![111u8; 8192];
    let value: Vec<u8> = vec![56u8; 18192];
    let enc_key = b"the_encryption_key".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::LZ4,
        );
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            Some(enc_key.to_vec()),
            CompressorType::LZ4,
        );
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

use digby::BlockSanity;
use digby::Db;
use digby::db_config::DbConfig;
use tempfile::TempDir;

#[test]
fn test_db_store_value_with_encryption() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder()
            .block_sanity(BlockSanity::Aes128Gcm)
            .build();
        let mut db = Db::create(db_path, None, Some(enc_key.to_vec()), &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None, Some(enc_key.to_vec())).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
}

#[test]
#[should_panic(expected = "Block sanity failed for block 1")]
fn test_db_store_value_with_encryption_wrong_key() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder()
            .block_sanity(BlockSanity::Aes128Gcm)
            .build();
        let mut db = Db::create(db_path, None, Some(enc_key.to_vec()), &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None, Some(b"bad_encryption_key".to_vec())).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
}

#[test]
#[should_panic(expected = "Key is incorrect size")]
fn test_db_store_value_with_encryption_no_key() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder()
            .block_sanity(BlockSanity::Aes128Gcm)
            .build();
        let mut db = Db::create(db_path, None, Some(enc_key.to_vec()), &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        Db::open(db_path, None, None).unwrap();
    }
}

#[test]
fn test_db_store_large_key_value_compressible_encryption() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key: Vec<u8> = vec![111u8; 8192];
    let value: Vec<u8> = vec![56u8; 18192];
    let enc_key = b"the_encryption_key".to_vec();
    {
        let db_config = DbConfig::builder()
            .block_sanity(BlockSanity::Aes128Gcm)
            .build();
        let mut db = Db::create(db_path, None, Some(enc_key.to_vec()), &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None, Some(enc_key.to_vec())).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::open(db_path, None, Some(enc_key.to_vec())).unwrap();
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
}

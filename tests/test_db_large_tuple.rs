use digby::Db;
use digby::db_config::DbConfig;
use rand::RngCore;
use rand::prelude::SliceRandom;
use rand::rng;
use tempfile::TempDir;

#[test]
fn test_db_store_large_key_value_incompressible() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let mut key: Vec<u8> = vec![0u8; 8192];
    let mut value: Vec<u8> = vec![0u8; 18192];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut key);
    rng.fill_bytes(&mut value);
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
}

#[test]
fn test_db_store_small_key_large_value_incompressible() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let mut key: Vec<u8> = vec![0u8; 32];
    let mut value: Vec<u8> = vec![0u8; 18192];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut key);
    rng.fill_bytes(&mut value);
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
}

#[test]
fn test_db_store_large_key_value_compressible() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key: Vec<u8> = vec![111u8; 8192];
    let value: Vec<u8> = vec![56u8; 18192];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete(&key));
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        let returned_value = db.get(key.as_ref());
        assert!(returned_value.is_none());
    }
}

#[test]
fn test_db_clear_large_tuples() {
    let size = 32u64;
    let mut large_value = vec![0u8; 5000];
    rng().fill_bytes(&mut large_value);
    let mut numbers: Vec<u64> = (0..=size).collect();
    let mut rng = rng();
    numbers.shuffle(&mut rng);

    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        for i in &numbers {
            let mut key = vec![0u8; 512];
            key[0..8].copy_from_slice(i.to_be_bytes().as_ref());
            db.put(&key, &large_value);
        }
    }
    // The new scope essentially closes the DB - when Files run out of scope then
    // they are close, Rust bizairely does not allow error handling on close!
    {
        let mut db = Db::open(db_path, None).unwrap();
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
        let mut db = Db::open(db_path, None).unwrap();
        let mut numbers: Vec<u64> = (0..=size).collect();
        numbers.shuffle(&mut rng);
        for i in &numbers {
            let mut key = vec![0u8; 512];
            key[0..8].copy_from_slice(i.to_be_bytes().as_ref());
            let returned_value = db.get(&key);
            assert!(returned_value.is_none());
        }
    }
}

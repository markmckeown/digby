use digby::BlockSanity;
use digby::Db;
use digby::db_config::DbConfig;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_basic_mirror() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let mirror_path = dir.path().join("mirror.db");
    let mirror_db_path = mirror_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, Some(mirror_db_path), None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    assert!(Path::new(mirror_db_path).exists());
    assert!(fs::metadata(mirror_db_path).unwrap().len() > 0);
    assert!(Path::new(db_path).exists());
    assert!(fs::metadata(db_path).unwrap().len() > 0);
    assert!(fs::metadata(db_path).unwrap().len() == fs::metadata(mirror_db_path).unwrap().len());
}

#[test]
fn test_corrupt_primary() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let mirror_path = dir.path().join("mirror.db");
    let mirror_db_path = mirror_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, Some(mirror_db_path), None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    {
        // Seek into the master page on the primary and corrupt it.
        let mut file = OpenOptions::new()
            .write(true)
            .open(db_path)
            .expect("File open failed.");
        file.seek(SeekFrom::Start(5000)).expect("File seek failed.");
        let my_byte: u8 = 42;
        file.write_all(&[my_byte]).expect("File write failed.");
    }
    {
        // Attempt to get the value without the mirror, should panic
        let result = std::panic::catch_unwind(|| {
            Db::open(db_path, None, None).unwrap();
        });
        match result {
            Ok(_) => panic!("Db should throw as corrupt"),
            Err(_) => (),
        }
    }
    {
        // Can get the value if the mirror is available.
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    {
        // The primary should be repaired - do not use mirror.
        let mut db = Db::open(db_path, None, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    assert!(Path::new(mirror_db_path).exists());
    assert!(fs::metadata(mirror_db_path).unwrap().len() > 0);
    assert!(Path::new(db_path).exists());
    assert!(fs::metadata(db_path).unwrap().len() > 0);
    assert!(fs::metadata(db_path).unwrap().len() == fs::metadata(mirror_db_path).unwrap().len());
}

#[test]
fn test_corrupt_primary_enc() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let mirror_path = dir.path().join("mirror.db");
    let mirror_db_path = mirror_path.to_str().unwrap();

    let enc_key = b"the_encryption_key".to_vec();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder()
            .block_sanity(BlockSanity::Aes128Gcm)
            .build();
        let mut db = Db::create(
            db_path,
            Some(mirror_db_path),
            Some(enc_key.to_vec()),
            &db_config,
        )
        .unwrap();
        db.put(key.as_ref(), value.as_ref());
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    {
        // Seek into the master page on the primary and corrupt it.
        let mut file = OpenOptions::new()
            .write(true)
            .open(db_path)
            .expect("File open failed.");
        file.seek(SeekFrom::Start(5000)).expect("File seek failed.");
        let my_byte: u8 = 42;
        file.write_all(&[my_byte]).expect("File write failed.");
    }
    {
        // Attempt to get the value without the mirror, should panic
        let result = std::panic::catch_unwind(|| {
            Db::open(db_path, None, Some(enc_key.to_vec())).unwrap();
        });
        match result {
            Ok(_) => panic!("Db should throw as corrupt"),
            Err(_) => (),
        }
    }
    {
        // Can get the value if the mirror is available.
        let mut db = Db::open(db_path, Some(mirror_db_path), Some(enc_key.to_vec())).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    {
        // The primary should be repaired - do not use mirror.
        let mut db = Db::open(db_path, None, Some(enc_key.to_vec())).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    assert!(Path::new(mirror_db_path).exists());
    assert!(fs::metadata(mirror_db_path).unwrap().len() > 0);
    assert!(Path::new(db_path).exists());
    assert!(fs::metadata(db_path).unwrap().len() > 0);
    assert!(fs::metadata(db_path).unwrap().len() == fs::metadata(mirror_db_path).unwrap().len());
}

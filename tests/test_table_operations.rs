use digby::Db;
use digby::compressor::CompressorType;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn test_db_create_table() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = b"the_table".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Cannot handle keys larger than u8::MAX.")]
fn test_db_create_table_name_too_big_get() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 257];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_create_table_name_too_big_create() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 257];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        db.create_table(name.as_ref());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_create_table_name_too_big_put() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 257];
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_clear_table_name_too_big_put() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 257];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        db.clear_table_with_delete(name.as_ref(), true);
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_clear_table_name_that_does_not_exist() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 25];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        db.clear_table_with_delete(name.as_ref(), true);
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_clear_table_name_that_does_not_exist_without_delete() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let name = vec![b'a'; 25];
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        db.clear_table_with_delete(name.as_ref(), false);
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_create_put_table_create_table() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        // Attmpt to delete from a table that does not exist - should return false but not panic
        assert!(!db.delete_table_entry(name.as_ref(), key.as_ref()));
        assert!(db.get_table_tree_root(name.as_ref()).is_none());

        // Do not explicitly create the table, just put an entry in it - this should implicitly create the table
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Attempt to delete a key that does not exist - should return false but not panic
        assert!(!db.delete_table_entry(name.as_ref(), b"the_non_existent_key".as_ref()));
        let ver_large_key = vec![b'a'; 655];
        assert!(!db.delete_table_entry(name.as_ref(), ver_large_key.as_ref()));
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_create_put_table() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_table_clear() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
        db.clear_table(name.as_ref());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref());
        assert!(returned_value.is_none());
        db.delete_table(name.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

#[test]
fn test_db_create_put_delete_table() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete_table_entry(name.as_ref(), key.as_ref()))
    }
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref());
        assert!(returned_value.is_none());
    }
    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

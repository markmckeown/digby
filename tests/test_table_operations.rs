use digby::Db;
use digby::db_config::DbConfig;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_db_create_table() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let name = b"the_table".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
}

#[test]
#[should_panic(expected = "Cannot handle keys larger than u8::MAX.")]
fn test_db_create_table_name_too_big_get() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let name = vec![b'a'; 257];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_create_table_name_too_big_create() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let name = vec![b'a'; 257];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.create_table(name.as_ref());
    }
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_create_table_name_too_big_put() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let name = vec![b'a'; 257];
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
    }
}

#[test]
#[should_panic(expected = "Cannot handle table name larger than u8::MAX.")]
fn test_db_clear_table_name_too_big_put() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let name = vec![b'a'; 257];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.clear_table_with_delete(name.as_ref(), true);
    }
}

#[test]
fn test_db_clear_table_name_that_does_not_exist() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let name = vec![b'a'; 25];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.clear_table_with_delete(name.as_ref(), true);
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
}

#[test]
fn test_db_clear_table_name_that_does_not_exist_without_delete() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let name = vec![b'a'; 25];
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        db.clear_table_with_delete(name.as_ref(), false);
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
}

#[test]
fn test_db_create_put_table_create_table() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
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
        let mut db = Db::open(db_path, None).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Attempt to delete a key that does not exist - should return false but not panic
        assert!(!db.delete_table_entry(name.as_ref(), b"the_non_existent_key".as_ref()));
        let ver_large_key = vec![b'a'; 655];
        assert!(!db.delete_table_entry(name.as_ref(), ver_large_key.as_ref()));
    }
}

#[test]
fn test_db_create_put_table() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
    }
}

#[test]
fn test_db_table_clear() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
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
        let mut db = Db::open(db_path, None).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
    }
}

#[test]
fn test_db_create_put_delete_table() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let name = b"the_table".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_none());
        db.create_table(name.as_ref());
        db.put_table_entry(name.as_ref(), key.as_ref(), value.as_ref());
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref()).unwrap();
        assert!(returned_value == value);
        assert!(db.delete_table_entry(name.as_ref(), key.as_ref()))
    }
    {
        let mut db = Db::open(db_path, None).unwrap();
        assert!(db.get_table_tree_root(name.as_ref()).is_some());
        let returned_value = db.get_table_entry(name.as_ref(), key.as_ref());
        assert!(returned_value.is_none());
    }
}

use digby::Db;
use digby::db_config::DbConfig;
use tempfile::TempDir;

#[test]
fn test_basic_transaction() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, &db_config).unwrap();
        let mut tx_ctx = db.new_transaction();
        db.put_txn(key.as_ref(), value.as_ref(), &mut tx_ctx);
        // Outside the scope of the txn the key is not in the DB.
        assert!(db.get(key.as_ref()).is_none());
        // Inside the txn scope the key is in the DB.
        assert!(db.get_txn(key.as_ref(), &tx_ctx).is_some());
        db.commit(&mut tx_ctx);
        // After the txn is committed the key is available.
        assert!(db.get(key.as_ref()).is_some());
    }
}

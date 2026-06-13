use digby::Db;
use digby::compressor::CompressorType;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn test_basic_transaction() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");

    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    {
        let mut db = Db::new(
            temp_file.path().to_str().unwrap(),
            None,
            CompressorType::None,
        );
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

    fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
}

use digby::Db;
use digby::db_config::DbConfig;
use digby::file_layer::FileLayer;
use digby::page::PageTrait;
use digby::page::PageType;
use digby::page_cache::PageCache;
use digby::page_container_layer::PageContainerLayer;
use digby::page_no::PageNo;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use tempfile::TempDir;

#[test]
#[should_panic(expected = "Block sanity failed for block 12, PageNoParityError")]
fn test_db_lost_write() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let db_config = DbConfig::builder().build();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let new_value = b"new_the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Open the db file and grab a page that will be reused in the next put.
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();
        let file_layer: FileLayer = FileLayer::new(db_file, db_config.block_size);
        let pg_ctr_layer = PageContainerLayer::open(file_layer, None, db_config, None);
        let mut page_cache: PageCache = PageCache::new(pg_ctr_layer);
        // This block will be the root page of the global tree after the next update.
        // The next put will overwrite it - so we grab a copy now, then do the update
        // and write this page back. It will look like a lost update for the page
        // during the put.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version.
        page_cache.put_page(&mut root_page);
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        // This will cause a panic as page at 12 parity will be wrong - the write to
        // it has been lost.
        let _ = db.get(key.as_ref()).unwrap();
    }
}

#[test]
fn test_db_lost_write_with_mirror() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let mirror_path = dir.path().join("mirror.db");
    let mirror_db_path = mirror_path.to_str().unwrap();

    let db_config = DbConfig::builder().build();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let new_value = b"new_the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, Some(mirror_db_path), None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Open the db file and grab a page that will be reused in the next put.
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();
        let file_layer: FileLayer = FileLayer::new(db_file, db_config.block_size);
        let pg_ctr_layer = PageContainerLayer::open(file_layer, None, db_config, None);
        let mut page_cache: PageCache = PageCache::new(pg_ctr_layer);
        // This block will be the root page of the global tree after the next update.
        // The next put will overwrite it - so we grab a copy now, then do the update
        // and write this page back. It will look like a lost update for the page
        // during the put.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version.
        page_cache.put_page(&mut root_page);
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        // Will get page from mirror file
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == new_value);
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        // Primary should be repaired
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == new_value);
    }
}

#[test]
#[should_panic(expected = "Block sanity failed for block 12, PageNoBlockOffsetError")]
fn test_db_mis_directed_write() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();

    let db_config = DbConfig::builder().build();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let new_value = b"new_the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, None, None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Open the db file and grab a page that will be reused in the next put.
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();
        let file_layer: FileLayer = FileLayer::new(
            db_file.try_clone().expect("File clone failed"),
            db_config.block_size,
        );
        let pg_ctr_layer = PageContainerLayer::open(file_layer, None, db_config, None);
        let mut page_cache: PageCache = PageCache::new(pg_ctr_layer);
        // This block will be the root page of the global tree after the next update.
        // The next put will overwrite it - so we grab a copy now.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version.

        root_page.set_page_number(PageNo::new(PageType::LeafPage, 0, 8));
        db_config
            .block_sanity
            .set_block_sanity(&mut root_page, &Vec::new());
        // Directly write the page to file with the wrong embedded offset
        let offset = 12 * db_config.block_size as u64;
        db_file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        db_file
            .write_all(root_page.get_pg_ctr_bytes())
            .expect("Failed to write for append_new_page");
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        // This will panic as the page read from offset 8 will have the
        // wrong embedded offset.
        let _ = db.get(key.as_ref()).unwrap();
    }
}

#[test]
fn test_db_mis_directed_write_mirror() {
    let dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = dir.path().join("db");
    let db_path = file_path.to_str().unwrap();
    let mirror_path = dir.path().join("mirror.db");
    let mirror_db_path = mirror_path.to_str().unwrap();

    let db_config = DbConfig::builder().build();
    let key = b"the_key".to_vec();
    let value = b"the_value".to_vec();
    let new_value = b"new_the_value".to_vec();
    {
        let db_config = DbConfig::builder().build();
        let mut db = Db::create(db_path, Some(mirror_db_path), None, &db_config).unwrap();
        db.put(key.as_ref(), value.as_ref());
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == value);

        // Open the db file and grab a page that will be reused in the next put.
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();
        let file_layer: FileLayer = FileLayer::new(
            db_file.try_clone().expect("File clone failed"),
            db_config.block_size,
        );
        let pg_ctr_layer = PageContainerLayer::open(file_layer, None, db_config, None);
        let mut page_cache: PageCache = PageCache::new(pg_ctr_layer);
        // This block will be the root page of the global tree after the next update.
        // The next put will overwrite it - so we grab a copy now.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version.

        root_page.set_page_number(PageNo::new(PageType::LeafPage, 0, 8));
        db_config
            .block_sanity
            .set_block_sanity(&mut root_page, &Vec::new());
        // Directly write the page to file with the wrong embedded offset
        let offset = 12 * db_config.block_size as u64;
        db_file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        db_file
            .write_all(root_page.get_pg_ctr_bytes())
            .expect("Failed to write for append_new_page");
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        // Get page from mirror file and repair
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == new_value);
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        // No mirror, but primary should be repaired.
        let returned_value = db.get(key.as_ref()).unwrap();
        assert!(returned_value == new_value);
    }
}

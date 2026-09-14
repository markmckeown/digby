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

//
// See "Parity Lost and Parity Regained" for model of
// error scenarios. These tests cover lost writes
// and mis-directed writes, tests in other files
// cover checksum mis-matches.
//

// Test a lost write can be detected, without a mirror this will cause
// a Panic as DB is corrupt.
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

        // Open the db file and grab a page that will be reused in the next put,
        // we know by debugging that the page at offset 12 will be used in the
        // next put.
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
        // and write this page back. Simulates a lost update for the page
        // during the put.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version - the page has not
        // changed so the checksum and block offset in the page are correct.
        page_cache.put_page(&mut root_page);
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, None, None).unwrap();
        // This will cause a panic as page at offset 12 will have the wrong parity - the write to
        // it has been lost. The parity encoded in the page pointer at offset 12 will have a parity
        // that does not match the parity inside the page.
        let _ = db.get(key.as_ref()).unwrap();
    }
}

// This repeats the same test as before except the db has a mirror. The page at
// offset will be found to have the wrong parity, however the page at the mirror
// will have the correct parity - so it will be used to repair the primary.
// We can test the primary is repaired by opening the db after the repair without
// the mirror.
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

// Lost write and corrupt mirror will cause a panic. Lose the write
// to both primary and mirror
#[test]
#[should_panic(expected = "Mirror page at offset 12, embedded page number does not match.")]
fn test_db_lost_write_corrupt_mirror() {
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

        // Open the db file and grab a page that will be reused in the next put,
        // we know by debugging that the page at offset 12 will be used in the
        // next put.
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();
        let mirror_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(mirror_db_path)
            .unwrap();
        let file_layer: FileLayer = FileLayer::new(db_file, db_config.block_size);
        let mirror_layer: FileLayer = FileLayer::new(mirror_file, db_config.block_size);
        let pg_ctr_layer =
            PageContainerLayer::open(file_layer, Some(mirror_layer), db_config, None);
        let mut page_cache: PageCache = PageCache::new(pg_ctr_layer);
        // This block will be the root page of the global tree after the next update.
        // The next put will overwrite it - so we grab a copy now, then do the update
        // and write this page back. Simulates a lost update for the page
        // during the put.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());
        // Now replace the page at offset 12 with the old version - the page has not
        // changed so the checksum and block offset in the page are correct.
        // This will overwrite both primary and mirror
        page_cache.put_page(&mut root_page);
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        // This will cause a panic as page at offset 12 will have the wrong parity - the write to
        // it has been lost. The parity encoded in the page pointer at offset 12 will have a parity
        // that does not match the parity inside the page.
        let _ = db.get(key.as_ref()).unwrap();
    }
}

// This tests a mis-directed write, for example a page is written to
// the wrong place by a faulty controller. This leads to two bad
// pages - there is a lost write for the page at the original target
// location and the page at the wrong location will be overwritten
// with the incorrect page for the location. The lost write is covered
// above.
// When reading a page the block offset within the page is compared
// to the page pointer used to access the page - if they do not match
// and there is no mirror then there should be a panic.
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

        // This block will be the root page of the global tree after the last update.
        // We directly read this page, change the block offset of the page that
        // that is stored in the page and write the page directly back to the file.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());

        // Set the page to have an block offset of 8
        root_page.set_page_number(PageNo::new(PageType::LeafPage, 0, 8));
        // Rebuild the checksum in the page after changing the embedded page number.
        db_config
            .block_sanity
            .set_block_sanity(&mut root_page, &Vec::new());
        // Directly write the page to file to offset 12 - we know this page
        // will be read on any get to the global tree.
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
        // This will panic as the page read from offset 12 will have the
        // wrong embedded offset (offset will be 8)
        let _ = db.get(key.as_ref()).unwrap();
    }
}

// Repeat of the previous test but with a mirror, this will prevent the panic
// and allow the primary to be repaired.
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

        // This block will be the root page of the global tree after the last update.
        // Grab a copy, modify the block offset embedded in the page and write directly
        // back to the primary.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());

        // Change the block offset in the page to 8.
        root_page.set_page_number(PageNo::new(PageType::LeafPage, 0, 8));
        // Update the checksum in the page.
        db_config
            .block_sanity
            .set_block_sanity(&mut root_page, &Vec::new());
        // Directly write the page to file but at the wrong location.
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

// The offset is changed within a page to simulate a mis-directed write. The
// mirros is also corrupted.
#[test]
#[should_panic(expected = "Mirror page at offset 12, embedded page number does not match.")]
fn test_db_mis_directed_write_corrupt_mirror() {
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

        // This block will be the root page of the global tree after the last update.
        // We directly read this page, change the block offset of the page that
        // that is stored in the page and write the page directly back to the file.
        let mut root_page = page_cache.get_page(PageNo::new(PageType::LeafPage, 0, 12));

        // Now do an update that will use the page at offset 12
        db.put(key.as_ref(), new_value.as_ref());

        // Set the page to have an block offset of 8
        root_page.set_page_number(PageNo::new(PageType::LeafPage, 0, 8));
        // Rebuild the checksum in the page after changing the embedded page number.
        db_config
            .block_sanity
            .set_block_sanity(&mut root_page, &Vec::new());
        // Directly write the page to file to offset 12 - we know this page
        // will be read on any get to the global tree.
        let offset = 12 * db_config.block_size as u64;
        db_file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        db_file
            .write_all(root_page.get_pg_ctr_bytes())
            .expect("Failed to write for append_new_page");
        let mut mirror_db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(mirror_db_path)
            .unwrap();
        mirror_db_file
            .seek(SeekFrom::Start(offset))
            .expect("Failed to seek for append_new_page");
        mirror_db_file
            .write_all(root_page.get_pg_ctr_bytes())
            .expect("Failed to write for append_new_page");
        page_cache.sync_all();
    }
    {
        let mut db = Db::open(db_path, Some(mirror_db_path), None).unwrap();
        // This will panic as the page read from offset 12 will have the
        // wrong embedded offset (offset will be 8)
        let _ = db.get(key.as_ref()).unwrap();
    }
}

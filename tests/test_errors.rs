// Tests for errors from creating and opening a database, and for handling
// corrupt pages.
mod common;
use digby::block_sanity::BlockSanity;
use digby::db::DigbyError;
use digby::db_config::DbConfig;
use digby::page::{PageTrait, PageType};
use digby::{Db, DbMasterPage, DbRootPage, FileLayer, PageContainerLayer, PageNo};
use std::os::unix::fs::FileExt;
use tempfile::TempDir;

const BLOCK_SIZE: u64 = 4096;

fn paths(dir: &TempDir) -> (String, String) {
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let mirror = dir.path().join("mirror").to_str().unwrap().to_string();
    (path, mirror)
}

fn create_db(path: &str, mirror: Option<&str>) {
    let mut db = Db::create(path, mirror, None, &DbConfig::builder().build()).unwrap();
    for i in 0..50u32 {
        db.put(format!("k{:04}", i).as_bytes(), &[1u8; 1000]);
    }
}

// Run f with a page container layer on the database file, bypassing Db so
// pages can be modified directly.
fn with_page_container(path: &str, f: impl FnOnce(&mut PageContainerLayer)) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let db_config = DbRootPage::read_db_config(&mut file);
    let file_layer = FileLayer::new(file, db_config.block_size);
    let mut pg_ctr_layer = PageContainerLayer::open(file_layer, None, db_config, None);
    f(&mut pg_ctr_layer);
    pg_ctr_layer.sync_data();
}

fn current_master(pg_ctr_layer: &mut PageContainerLayer) -> DbMasterPage {
    let master1 =
        DbMasterPage::from_page(pg_ctr_layer.read_page(PageNo::new(PageType::DbMaster, 0, 1)));
    let master2 =
        DbMasterPage::from_page(pg_ctr_layer.read_page(PageNo::new(PageType::DbMaster, 0, 2)));
    if master1.get_version() > master2.get_version() {
        master1
    } else {
        master2
    }
}

fn corrupt_block(path: &str, blk_offset: u64) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0xAB; 64], blk_offset * BLOCK_SIZE + 100)
        .unwrap();
}

// Create errors.

#[test]
fn test_create_aes_without_key() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    let db_config = DbConfig::builder()
        .block_sanity(BlockSanity::Aes128Gcm)
        .build();
    let result = Db::create(&path, None, None, &db_config);
    assert!(matches!(result, Err(DigbyError::ConfigurationError)));
    assert!(!std::path::Path::new(&path).exists());
}

#[test]
fn test_create_xxhash32_with_key() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    let db_config = DbConfig::builder()
        .block_sanity(BlockSanity::XxH32Checksum)
        .build();
    let result = Db::create(&path, None, Some(b"0123456789abcdef".to_vec()), &db_config);
    assert!(matches!(result, Err(DigbyError::ConfigurationError)));
}

#[test]
fn test_create_xxhash64_with_key() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    let db_config = DbConfig::builder()
        .block_sanity(BlockSanity::XxH64Checksum)
        .build();
    let result = Db::create(&path, None, Some(b"0123456789abcdef".to_vec()), &db_config);
    assert!(matches!(result, Err(DigbyError::ConfigurationError)));
}

#[test]
fn test_create_file_exists() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    std::fs::write(&path, b"existing").unwrap();
    let result = Db::create(&path, None, None, &DbConfig::builder().build());
    assert!(matches!(result, Err(DigbyError::FileExistsAtPath(p)) if p == path));
    // The existing file is not touched.
    assert_eq!(std::fs::read(&path).unwrap(), b"existing");
}

#[test]
fn test_create_mirror_exists() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, mirror) = paths(&dir);
    std::fs::write(&mirror, b"existing").unwrap();
    let result = Db::create(&path, Some(&mirror), None, &DbConfig::builder().build());
    assert!(matches!(result, Err(DigbyError::FileExistsAtPath(p)) if p == mirror));
    assert_eq!(std::fs::read(&mirror).unwrap(), b"existing");
}

// Open errors.

#[test]
fn test_open_missing_file() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    let result = Db::open(&path, None, None);
    assert!(matches!(result, Err(DigbyError::FileNotExistsAtPath(p)) if p == path));
}

#[test]
fn test_open_empty_file() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    std::fs::write(&path, b"").unwrap();
    let result = Db::open(&path, None, None);
    assert!(matches!(result, Err(DigbyError::DbFileEmptyAtPath(p)) if p == path));
}

#[test]
fn test_open_missing_mirror() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, mirror) = paths(&dir);
    create_db(&path, Some(&mirror));
    std::fs::remove_file(&mirror).unwrap();
    let result = Db::open(&path, Some(&mirror), None);
    assert!(matches!(result, Err(DigbyError::FileNotExistsAtPath(p)) if p == mirror));
}

#[test]
fn test_open_empty_mirror() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, mirror) = paths(&dir);
    create_db(&path, Some(&mirror));
    std::fs::write(&mirror, b"").unwrap();
    let result = Db::open(&path, Some(&mirror), None);
    assert!(matches!(result, Err(DigbyError::DbFileEmptyAtPath(p)) if p == mirror));
}

// The database file is shorter than the committed block count.
#[test]
fn test_open_truncated_file() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    create_db(&path, None);
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - BLOCK_SIZE).unwrap();
    drop(file);
    let result = Db::open(&path, None, None);
    assert!(matches!(result, Err(DigbyError::DbFileTruncated(p)) if p == path));
}

// A database created before the block count was stored in the master page
// has a block count of zero, extra blocks at the end of its file are kept.
#[test]
fn test_open_without_committed_block_count() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    create_db(&path, None);
    with_page_container(&path, |pg_ctr_layer| {
        for blk_offset in [1u64, 2u64] {
            let page_no = PageNo::new(PageType::DbMaster, 0, blk_offset);
            let mut master = DbMasterPage::from_page(pg_ctr_layer.read_page(page_no));
            master.set_blk_count(0);
            pg_ctr_layer.write_page(master.get_page(), page_no);
        }
    });
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let len = file.metadata().unwrap().len() + 2 * BLOCK_SIZE;
    file.set_len(len).unwrap();
    drop(file);

    let mut db = Db::open(&path, None, None).unwrap();
    assert_eq!(db.get(b"k0049"), Some(vec![1u8; 1000]));
    drop(db);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
}

// Corruption.

// A corrupt page in the primary is repaired from the mirror, but if the
// mirror copy is also corrupt the page cannot be read.
#[test]
#[should_panic(expected = "from mirror, ChecksumMisMatch")]
fn test_page_corrupt_in_primary_and_mirror() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, mirror) = paths(&dir);
    create_db(&path, Some(&mirror));
    let mut root_blk_offset = 0;
    with_page_container(&path, |pg_ctr_layer| {
        root_blk_offset = current_master(pg_ctr_layer)
            .get_global_tree_root_page_no()
            .get_blk_offset();
    });
    corrupt_block(&path, root_blk_offset);
    corrupt_block(&mirror, root_blk_offset);

    let mut db = Db::open(&path, Some(&mirror), None).unwrap();
    db.get(b"k0000");
}

// A page at the right offset with the right version byte but the wrong page
// type is detected as a page number mismatch.
#[test]
#[should_panic(expected = "PageNoMisMatch")]
fn test_page_with_wrong_type() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    create_db(&path, None);
    with_page_container(&path, |pg_ctr_layer| {
        let root_page_no = current_master(pg_ctr_layer).get_global_tree_root_page_no();
        let mut page = pg_ctr_layer.read_page(root_page_no);
        let mut wrong_page_no = PageNo::from_u64(root_page_no.to_u64());
        let wrong_type = if root_page_no.get_pg_type() == PageType::DirPage {
            PageType::LeafPage
        } else {
            PageType::DirPage
        };
        wrong_page_no.set_type(wrong_type);
        page.set_page_number(wrong_page_no);
        pg_ctr_layer.write_page(&mut page, root_page_no);
    });

    let mut db = Db::open(&path, None, None).unwrap();
    db.get(b"k0000");
}

// The root page checksum is checked on open.
#[test]
#[should_panic(expected = "Root page checksum mismatch")]
fn test_open_corrupt_root_page() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    create_db(&path, None);
    // Corrupt bytes after the config fields so only the checksum is wrong.
    corrupt_block(&path, 0);
    let _ = Db::open(&path, None, None);
}

// The root page magic number is checked on open.
#[test]
#[should_panic(expected = "Invalid magic number for RootPage")]
fn test_open_root_page_bad_magic_number() {
    common::init_logger();
    let dir = TempDir::new().unwrap();
    let (path, _) = paths(&dir);
    create_db(&path, None);
    // Change the magic number and recompute the checksum.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut page = digby::Page::new(BLOCK_SIZE as usize, BLOCK_SIZE as usize - 4);
    file.read_exact_at(page.get_pg_ctr_bytes_mut(), 0).unwrap();
    page.get_pg_ctr_bytes_mut()[16..20].copy_from_slice(&1u32.to_le_bytes());
    digby::XxHashSanity::set_checksum(&mut page);
    file.write_all_at(page.get_pg_ctr_bytes(), 0).unwrap();
    drop(file);
    let _ = Db::open(&path, None, None);
}

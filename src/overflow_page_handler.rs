use crate::FreePageManager;
use crate::OverflowPage;
use crate::OverflowTuple;
use crate::PageCache;
use crate::PageNo;
use crate::page::PageTrait;
use crate::tuple::Overflow;
use crate::tuple::Tuple;
use crate::tuple::TupleTrait;

pub struct OverflowPageHandler {}

impl OverflowPageHandler {
    pub fn store_overflow_tuple(
        tuple: OverflowTuple,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        version: u64,
    ) -> PageNo {
        // We write the buffer backwards as we want to create a linked list
        // of pages. The last page we write will be the head of the list
        // and contain the start of the OverflowTuple.
        let buffer = tuple.get_serialized();
        let mut end = tuple.get_byte_size();

        let mut previous = PageNo::from_u64(0);
        let mut next_page: PageNo;
        loop {
            // TODO - pick block size
            next_page = free_pg_mgr.get_free_page(page_cache, 0);
            let mut page = OverflowPage::create_new(page_cache.get_db_config(), next_page, version);
            page.set_next_page(previous);

            let free_space = page.get_free_space();
            let bytes_to_write: usize = if end < free_space { end } else { free_space };
            page.add_bytes(&buffer[end - bytes_to_write..end], bytes_to_write);
            page_cache.put_page(page.get_page());
            end -= bytes_to_write;
            if end == 0 {
                break;
            }
            previous = next_page;
        }

        next_page
    }

    pub fn store_overflow_tuple2(
        tuple: OverflowTuple,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
        version: u64,
    ) -> PageNo {
        // We write the buffer backwards as we want to create a linked list
        // of pages. The last page we write will be the head of the list
        // and contain the start of the OverflowTuple.
        let buffer = tuple.get_serialized();
        let mut end = tuple.get_byte_size();

        let mut previous = PageNo::new(0, 0);
        let mut next_page: PageNo = PageNo::new(0, 0);

        let max_block_size = page_cache.get_db_config().get_max_overflow_pg_free_space();

        let max_blocks = end / max_block_size;
        let remainder = end % max_block_size;

        for _ in 0..max_blocks {
            next_page = free_pg_mgr.get_free_page(
                page_cache,
                page_cache.get_db_config().get_max_overflow_exp_size(),
            );
            let mut page = OverflowPage::create_new(page_cache.get_db_config(), next_page, version);
            page.set_next_page(previous);

            let bytes_to_write: usize = max_block_size;
            page.add_bytes(&buffer[end - bytes_to_write..end], bytes_to_write);
            page_cache.put_page(page.get_page());
            end -= bytes_to_write;
            previous = next_page;
        }

        if remainder > 0 {
            let pg_exp = page_cache
                .get_db_config()
                .get_blk_cnt_shift_for_size(remainder);
            next_page = free_pg_mgr.get_free_page(page_cache, pg_exp);
            let mut page = OverflowPage::create_new(page_cache.get_db_config(), next_page, version);
            page.set_next_page(previous);

            let bytes_to_write: usize = remainder;
            page.add_bytes(&buffer[end - bytes_to_write..end], bytes_to_write);
            page_cache.put_page(page.get_page());
        }

        next_page
    }

    pub fn get_overflow_tuple(
        overflow_page_no: PageNo,
        page_cache: &mut PageCache,
    ) -> OverflowTuple {
        let mut buffer: Vec<u8> = Vec::new();

        let mut page_no = overflow_page_no;
        loop {
            let page = OverflowPage::from_page(page_cache.get_page(page_no));
            buffer.append(&mut page.get_tuple_bytes());
            page_no = page.get_next_page();
            if page_no.get_blk_offset() == 0 {
                break;
            }
        }
        OverflowTuple::from_bytes(buffer)
    }

    pub fn delete_overflow_tuple_pages(
        tuple_option: Option<Tuple>,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
    ) -> u32 {
        if tuple_option.is_none() {
            return 0;
        }
        let tuple = tuple_option.unwrap();
        if tuple.get_overflow() == Overflow::None {
            return 0;
        }
        // A tuple has been deleted that points to a overflow page.
        let page_no = PageNo::from_bytes(tuple.get_value());
        OverflowPageHandler::delete_overflow_pages(page_no, page_cache, free_pg_mgr)
    }

    pub fn delete_overflow_pages(
        first_page: PageNo,
        page_cache: &mut PageCache,
        free_pg_mgr: &mut FreePageManager,
    ) -> u32 {
        free_pg_mgr.return_free_page_no(page_cache, first_page);
        let mut page_no = first_page;
        let mut count: u32 = 1;
        loop {
            let page = OverflowPage::from_page(page_cache.get_page(page_no));
            page_no = page.get_next_page();
            if page_no.get_blk_offset() == 0 {
                break;
            }
            free_pg_mgr.return_free_page_no(page_cache, page_no);
            count += 1;
        }

        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DbMasterPage, db_config::DbConfig};

    const DB_CONFIG: DbConfig = DbConfig::builder()
        .block_size(4096)
        .block_sanity_size(4)
        .compressor_type(crate::compressor::CompressorType::None)
        .build();

    #[test]
    fn store_overflow_tuple() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        // Create file for db
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&temp_file)
            .expect("Failed to open or create DB file");

        let version: u64 = 89;
        let new_version: u64 = 90;

        // Set up the page_cache
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, DB_CONFIG.block_size);
        let block_layer: crate::PageContainerLayer =
            crate::PageContainerLayer::new(file_layer, DB_CONFIG);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        // Setup the free page infrastructure
        let _ = page_cache.generate_free_pages(11, 0);

        let mut master_page = DbMasterPage::create_new(&DB_CONFIG, PageNo::new(0, 1), version);
        let offset = 2;
        for i in 0..9 {
            let mut free_dir_page = crate::FreeDirPage::create_new(
                page_cache.get_db_config(),
                PageNo::new(0, offset + i),
                0,
            );
            page_cache.put_page(free_dir_page.get_page());
            master_page.set_free_page_dir_page_no(i as u8, PageNo::new(0, offset + i as u64));
        }

        let mut free_pg_mgr = FreePageManager::new(&master_page, &mut page_cache, new_version);

        let key: Vec<u8> = vec![111u8; 8192];
        let value: Vec<u8> = vec![56u8; 18192];
        let tuple = OverflowTuple::new(&key, &value, new_version, Overflow::KeyValueOverflow);

        let overflow_tuple_page_no = OverflowPageHandler::store_overflow_tuple2(
            tuple,
            &mut page_cache,
            &mut free_pg_mgr,
            new_version,
        );

        let reloaded_tuple =
            OverflowPageHandler::get_overflow_tuple(overflow_tuple_page_no, &mut page_cache);
        assert_eq!(reloaded_tuple.get_version(), 90);
        assert_eq!(reloaded_tuple.get_key(), key);
        assert_eq!(reloaded_tuple.get_value(), value);

        let tuple_no_overflow = Tuple::new(&key[0..10], &value[0..10], 1);
        let count = OverflowPageHandler::delete_overflow_tuple_pages(
            None,
            &mut page_cache,
            &mut free_pg_mgr,
        );
        assert_eq!(count, 0);
        let count = OverflowPageHandler::delete_overflow_tuple_pages(
            Some(tuple_no_overflow.clone()),
            &mut page_cache,
            &mut free_pg_mgr,
        );
        assert_eq!(count, 0);

        let page_no_bytes = overflow_tuple_page_no.get_bytes();
        let overflow_tuple_val =
            Tuple::new_with_overflow(&key[0..10], &page_no_bytes, 1, Overflow::KeyValueOverflow);
        let count = OverflowPageHandler::delete_overflow_tuple_pages(
            Some(overflow_tuple_val.clone()),
            &mut page_cache,
            &mut free_pg_mgr,
        );
        assert!(count > 0);

        // Flush the free pages.
        free_pg_mgr.flush_free_page_trackers(&mut master_page, &mut page_cache);

        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}

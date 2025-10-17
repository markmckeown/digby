use crate::page::PageTrait;
use crate::tuple::TupleTrait;
use crate::tuple::Tuple;
use crate::FreePageTracker;
use crate::OverflowPage;
use crate::OverflowTuple;
use crate::PageCache;
use crate::tuple::Overflow;

pub struct OverflowPageHandler {

}

impl OverflowPageHandler {
    pub fn store_overflow_tuple(
        tuple: OverflowTuple,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        version: u64
    ) -> u32 {
        // We write the buffer backwards as we want to create a linked list
        // of pages. The last page we write will be the head of the list
        // and contain the start of the OverflowTuple.
        let buffer = tuple.get_serialized();
        let mut end = tuple.get_byte_size();

        let mut previous: u32 = 0;
        let mut next_page: u32;
        loop {
            next_page = free_page_tracker.get_free_page(page_cache);
            let mut page = OverflowPage::create_new(page_cache.get_page_config(), next_page, version);
            page.set_next_page(previous);

            let free_space = page.get_free_space();
            let bytes_to_write: usize;
            if end < free_space {
                bytes_to_write = end;
            } else {
                bytes_to_write = free_space;
            }
            page.add_bytes(&buffer[end - bytes_to_write .. end], bytes_to_write);
            page_cache.put_page(page.get_page());
            end = end - bytes_to_write;
            if end == 0 {
                break;
            }
            previous = next_page;
        }

        return next_page;
    }


    pub fn get_overflow_tuple(
        overflow_page_no: u32,
        page_cache: &mut PageCache) -> OverflowTuple {
        let mut buffer: Vec<u8> = Vec::new();

        let mut page_no = overflow_page_no;
        loop {
            let page = OverflowPage::from_page(page_cache.get_page(page_no));
            buffer.append(&mut page.get_tuple_bytes());
            page_no = page.get_next_page();
            if page_no == 0 {
                break;
            }
        }
        return OverflowTuple::from_bytes(buffer);
    }

    pub fn delete_overflow_pages(
        tuple_option: Option<Tuple>,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker
    ) -> u32 {
        if tuple_option.is_none() {
            return 0;
        }
        let tuple = tuple_option.unwrap();
        if *tuple.get_overflow() == Overflow::None {
            return 0;
        }
        // A tuple has been deleted that points to a overflow page.
        let mut page_no = u32::from_le_bytes(tuple.get_value().to_vec().try_into().unwrap());
        free_page_tracker.return_free_page_no(page_no);
        let mut count:u32 = 1;
        loop {
            let page = OverflowPage::from_page(page_cache.get_page(page_no));
            page_no = page.get_next_page();
            if page_no == 0 {
                break;
            }
            free_page_tracker.return_free_page_no(page_no);
            count = count + 1;
        }

        return count;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_overflow_tuple() {
        let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        // Create file for db
        let db_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&temp_file).expect("Failed to open or create DB file");
        
        let version: u64 = 89;
        let new_version: u64 = 90;

        // Set up the page_cache
        let file_layer: crate::FileLayer = crate::FileLayer::new(db_file, crate::Db::BLOCK_SIZE as usize);
        let block_layer: crate::BlockLayer = crate::BlockLayer::new(file_layer, crate::Db::BLOCK_SIZE as usize);
        let mut page_cache: crate::PageCache = crate::PageCache::new(block_layer);

        // Setup the free page infrastructure
        let free_dir_page_no = *page_cache.generate_free_pages(1).get(0).unwrap();
        let mut free_dir_page = crate::FreeDirPage::create_new(page_cache.get_page_config(), free_dir_page_no, version);
        page_cache.put_page(free_dir_page.get_page());
        let mut free_page_tracker = FreePageTracker::new(
            page_cache.get_page(free_dir_page_no), new_version, *page_cache.get_page_config());

        let key: Vec<u8> = vec![111u8; 8192];
        let value: Vec<u8> = vec![56u8; 18192];
        let tuple = OverflowTuple::new(&key, &value, new_version, Overflow::KeyValueOverflow);

        let overflow_tuple_page_no = OverflowPageHandler::store_overflow_tuple(tuple, &mut page_cache, 
            &mut free_page_tracker, new_version);
        
        // Flush the free pages.
        let free_pages = free_page_tracker.get_free_dir_pages(&mut page_cache);
        for mut free_page in free_pages {
            page_cache.put_page(free_page.get_page());
        }

        let reloaded_tuple = OverflowPageHandler::get_overflow_tuple(overflow_tuple_page_no, &mut page_cache);
        assert_eq!(reloaded_tuple.get_version(), 90);
        assert_eq!(reloaded_tuple.get_key(), key);
        assert_eq!(reloaded_tuple.get_value(), value);        

        std::fs::remove_file(temp_file.path()).expect("Failed to remove temp file");
    }
}
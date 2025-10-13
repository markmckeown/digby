use crate::page::PageTrait;
use crate::tuple::TupleTrait;
use crate::FreePageTracker;
use crate::OverflowPage;
use crate::OverflowTuple;
use crate::PageCache;

pub struct OverflowPageHandler {

}

impl OverflowPageHandler {
    pub fn store_overflow_tuple(
        tuple: OverflowTuple,
        page_cache: &mut PageCache,
        free_page_tracker: &mut FreePageTracker,
        version: u64,
        page_size: usize
    ) -> u32 {
        let buffer = tuple.get_serialized();
        let mut end = tuple.get_byte_size();

        let mut previous: u32 = 0;
        let mut next_page: u32;
        loop {
            next_page = free_page_tracker.get_free_page(page_cache);
            let mut page = OverflowPage::new(page_size as u64, next_page, version);
            page.set_next_page(previous);

            let free_space = page.get_free_space(page_size);
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
}
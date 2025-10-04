use crate::page::PageTrait;
use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};
use crate::free_page_tracker::FreePageTracker;
use crate::page_cache::PageCache;

pub struct LeafPageHandler {
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_tuple(page: TreeLeafPage, 
                    tuple: Tuple,
                    free_page_tracker: &mut FreePageTracker,
                    page_cache: &mut PageCache,
                    version: u64, 
                    page_size: usize) -> Vec<TreeLeafPage> {
        let mut pages: Vec<TreeLeafPage> = Vec::new();
        pages.push(page);
        LeafPageHandler::add_to_page(tuple, &mut pages, page_size);
        LeafPageHandler::map_pages(&mut pages, free_page_tracker, page_cache, version);
        pages
    }

    fn map_pages(pages: &mut Vec<TreeLeafPage>, 
                    free_page_tracker: &mut FreePageTracker, 
                    page_cache: &mut PageCache, 
                    version: u64) -> () {
        for page in pages {
            let old_page_no = page.get_page_number();
            if old_page_no != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page.set_page_number(new_page_no);
            page.set_version(version);
        }
    }

    
    fn add_to_page(tuple: Tuple, 
                new_pages: &mut Vec<TreeLeafPage>,
                page_size: usize) -> () {
        assert!(!new_pages.is_empty());
        let page = new_pages.last_mut().unwrap();

        if page.can_fit(tuple.get_byte_size()) {
            page.store_tuple(tuple, page_size);
            return;
        }

        let mut tuples_to_right = page.get_right_half_tuples(page_size);
        let mut new_page = TreeLeafPage::new(page_size as u64, 0);
        if tuples_to_right.is_empty() {
            // Edge case, the page cannot be split as it has only one entry
            assert!(new_page.can_fit(tuple.get_byte_size()));
            new_page.store_tuple(tuple, page_size);
            new_pages.push(new_page);
            return;
        }

        let left_key_for_new_page = &tuples_to_right.get(0).unwrap().get_key().to_vec()[..];
        new_page.add_sorted_tuples(&mut tuples_to_right, page_size);

        if tuple.get_key() < left_key_for_new_page {
            if page.can_fit(tuple.get_byte_size()) {
                page.store_tuple(tuple, page_size);
                new_pages.push(new_page);
                return;
            } else {
                new_pages.insert(0, new_page);
                return LeafPageHandler::add_to_page(tuple, new_pages, page_size);  
            }
        }

        if new_page.can_fit(tuple.get_byte_size()) {
            new_page.store_tuple(tuple, page_size);
            new_pages.push(new_page);
            return;
        }
        new_pages.push(new_page);
        return LeafPageHandler::add_to_page(tuple, new_pages, page_size);
    }
}

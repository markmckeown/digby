use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};

pub struct LeafPageHandler {
   
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_to_page(tuple: Tuple, 
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

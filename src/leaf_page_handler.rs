use crate::tree_leaf_page::TreeLeafPage;
use crate::tuple::{Tuple, TupleTrait};

pub struct LeafPageHandler {
    
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_to_page(page: &mut TreeLeafPage, tuple: Tuple, page_size: usize) -> Vec<TreeLeafPage> {
        let new_pages: Vec<TreeLeafPage> = Vec::new();
        if page.can_fit(tuple.get_byte_size()) {
            page.store_tuple(tuple, page_size);
            return new_pages;
        }

        // Tuple is too big. Need to split. Need to get the right half of the page
        // and put into another page.

        return new_pages;
    }
}
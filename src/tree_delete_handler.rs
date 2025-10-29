use crate::tuple::{Overflow, TupleTrait};
use crate::{FreePageTracker, OverflowPageHandler, Page, PageCache, TreeLeafPage};
use crate::page::{PageTrait, PageType};
pub struct TreeDeleteHandler {

}

impl TreeDeleteHandler {
    pub fn delete_key(key: &Vec<u8>, 
        root_page: Page, 
        page_cache: &mut PageCache, 
        free_page_tracker: &mut FreePageTracker, 
        new_version: u64) -> (u32, bool) {
        let root_page_no = root_page.get_page_number();

        if root_page.get_type() == PageType::TreeLeaf {
           // The root of the tree is actually a leaf page - requires special handling.
            let mut tree_root_single = TreeLeafPage::from_page(root_page);
            return TreeDeleteHandler::delete_key_from_root(key, &mut tree_root_single, 
                page_cache, free_page_tracker, new_version);
        }
        

        return (root_page_no, false)
    }


    fn delete_key_from_root(
        key: &Vec<u8>, 
        root_page: &mut TreeLeafPage, 
        page_cache: &mut PageCache, 
        free_page_tracker: &mut FreePageTracker, 
        new_version: u64) -> (u32, bool) {
        let root_page_no = root_page.get_page_number();
    
        let tuple = root_page.delete_key(key);
        if tuple.is_none() {
            return (root_page_no, false);
        }

        // Store the root page back into the page cache.
        free_page_tracker.return_free_page_no(root_page_no);
        let new_root_page_no = free_page_tracker.get_free_page(page_cache);
        root_page.set_page_number(new_root_page_no);
        root_page.set_version(new_version);
        page_cache.put_page(root_page.get_page());

        let tuple_unwrapped = tuple.unwrap();
        if tuple_unwrapped.get_overflow() == Overflow::None {
            return (new_root_page_no, true)
        }

        // Overflow page - need to delete overflows.
        OverflowPageHandler::delete_overflow_tuple_pages(Some(tuple_unwrapped), 
            page_cache, free_page_tracker);

        return (new_root_page_no, true);
    }
}
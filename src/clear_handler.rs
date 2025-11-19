use crate::TreeDirPage;
use crate::TreeLeafPage;
use crate::page::PageTrait;
use crate::page::Page;
use crate::page::PageType;
use crate::page_cache::PageCache;
use crate::free_page_tracker::FreePageTracker;
use crate::tuple::Overflow;
use crate::tuple::TupleTrait;
use crate::overflow_page_handler::OverflowPageHandler;

pub struct ClearHandler {
    // Currently empty - placeholder for future functionality
}   

impl ClearHandler {

    pub fn clear_tree(first: Page,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64) -> u64 {
        
        if first.get_type() == PageType::TreeLeaf {
            let root_leaf_page = TreeLeafPage::from_page(first);
            return ClearHandler::clear_root_leaf_page(root_leaf_page, free_page_tracker, page_cache, new_version);
        }    

        let root_dir_page = TreeDirPage::from_page(first);
        ClearHandler::clear_tree_dir_pages(root_dir_page, free_page_tracker, page_cache);
        return ClearHandler::create_new_root_page(free_page_tracker, page_cache, new_version);
    }


    pub fn clear_tree_dir_pages(dir_page: TreeDirPage, 
        free_page_tracker: &mut FreePageTracker, 
        page_cache: &mut PageCache) -> () {
        free_page_tracker.return_free_page_no(dir_page.get_page_number());  

        let dir_entries = dir_page.get_all_dir_entries();
        for dir_entry in dir_entries {
            let page = page_cache.get_page(dir_entry.get_page_no());
            if page.get_type() == PageType::TreeLeaf {
                ClearHandler::clear_leaf_page(TreeLeafPage::from_page(page), free_page_tracker, page_cache);
                continue;
            }
            // Recursion. May not be best approach here.
            ClearHandler::clear_tree_dir_pages(TreeDirPage::from_page(page), free_page_tracker, page_cache);
        }

    }

    pub fn create_new_root_page(free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64) -> u64 {
       let new_root_page_no = free_page_tracker.get_free_page(page_cache); 
       let mut new_root_page = TreeLeafPage::create_new(page_cache.get_page_config(), new_root_page_no);
       new_root_page.set_version(new_version);    
       page_cache.put_page(new_root_page.get_page());    
       return new_root_page_no;     
    }


    pub fn clear_root_leaf_page(root_page: TreeLeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64) -> u64 {

        ClearHandler::clear_leaf_page(root_page, free_page_tracker, page_cache);
        return ClearHandler::create_new_root_page(free_page_tracker, page_cache, new_version);
    }

    pub fn clear_leaf_page(page: TreeLeafPage, 
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache) -> () {

        let tuples = page.get_all_tuples();
        for tuple in tuples {
            if tuple.get_overflow() != Overflow::None {
                OverflowPageHandler::delete_overflow_tuple_pages(Some(tuple), page_cache, free_page_tracker);
            }
        }
        free_page_tracker.return_free_page_no(page.get_page_number());    
    }
}

use crate::LeafPage;
use crate::PageNo;
use crate::dir_page::DirPage;
use crate::free_page_tracker::FreePageTracker;
use crate::overflow_page_handler::OverflowPageHandler;
use crate::page::Page;
use crate::page::PageTrait;
use crate::page::PageType;
use crate::page_cache::PageCache;
use crate::tuple::Overflow;
use crate::tuple::TupleTrait;

pub struct ClearHandler {
    // Currently empty - placeholder for future functionality
}

// Functionality for clearing a tree - basically return
// all pages used in the tree as free pages and create
// a new root page for the tree.
//
// Need to be careful as the tree could contain references
// to overflow tuples that are stored in overflow pages -
// so before returning a leaf page as a free page need
// to make sure no entries are for overflow tuples. This
// will make clear a more expensive operation than it
// should be.
impl ClearHandler {
    pub fn clear_tree(
        first: Page,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        // If the root of the page is a leaf page, ie
        // only page in the tree then special case it.
        if first.get_type() == PageType::LeafPage {
            let root_leaf_page = LeafPage::from_page(first);
            return ClearHandler::clear_root_leaf_page(
                root_leaf_page,
                free_page_tracker,
                page_cache,
                new_version,
            );
        }

        let root_dir_page = DirPage::from_page(first);
        ClearHandler::clear_tree_dir_pages(root_dir_page, free_page_tracker, page_cache);
        ClearHandler::create_new_root_page(free_page_tracker, page_cache, new_version)
    }

    // Walk the tree recursively until hit leaf pages, clear
    // the leaf pages and return to free pages. Return
    // dir pages when they are empty to free pages.
    pub fn clear_tree_dir_pages(
        dir_page: DirPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
    ) {
        free_page_tracker.return_free_page_no(dir_page.get_page_number());

        let dir_entries = dir_page.get_all_child_pages();
        for dir_entry in dir_entries {
            let page = page_cache.get_page(PageNo::from_u64(dir_entry));
            if page.get_type() == PageType::LeafPage {
                ClearHandler::clear_leaf_page(
                    LeafPage::from_page(page),
                    free_page_tracker,
                    page_cache,
                );
                continue;
            }
            // Recursion. May not be best approach here.
            ClearHandler::clear_tree_dir_pages(
                DirPage::from_page(page),
                free_page_tracker,
                page_cache,
            );
        }
    }

    pub fn create_new_root_page(
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        let new_root_page_no = free_page_tracker.get_free_page(page_cache);
        let mut new_root_page = LeafPage::create_new(
            page_cache.get_page_config(),
            PageNo::new(0, new_root_page_no),
            new_version,
        );
        page_cache.put_page(new_root_page.get_page());
        new_root_page_no
    }

    pub fn clear_root_leaf_page(
        root_page: LeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
        ClearHandler::clear_leaf_page(root_page, free_page_tracker, page_cache);
        ClearHandler::create_new_root_page(free_page_tracker, page_cache, new_version)
    }

    // Clear a leaf page - cannot simply return the leaf page as free page
    // as it may hold references to tuples in the overflow pages.
    pub fn clear_leaf_page(
        page: LeafPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
    ) {
        let tuples = page.get_all_tuples();
        for tuple in tuples {
            if tuple.get_overflow() != Overflow::None {
                OverflowPageHandler::delete_overflow_tuple_pages(
                    Some(tuple),
                    page_cache,
                    free_page_tracker,
                );
            }
        }
        free_page_tracker.return_free_page_no(page.get_page_number());
    }
}

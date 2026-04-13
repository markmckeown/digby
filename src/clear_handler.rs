use crate::LeafPage;
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

impl ClearHandler {
    pub fn clear_tree(
        first: Page,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        new_version: u64,
    ) -> u64 {
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

    pub fn clear_tree_dir_pages(
        dir_page: DirPage,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
    ) {
        free_page_tracker.return_free_page_no(dir_page.get_page_number());

        let dir_entries = dir_page.get_all_child_pages();
        for dir_entry in dir_entries {
            let page = page_cache.get_page(dir_entry);
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
        let mut new_root_page =
            LeafPage::create_new(page_cache.get_page_config(), new_root_page_no, new_version);
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

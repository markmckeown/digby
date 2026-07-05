use crate::free_page_tracker::FreePageTracker;
use crate::leaf_page::LeafPage;
use crate::page::PageTrait;
use crate::page_cache::PageCache;
use crate::tuple::{Tuple, TupleTrait};

pub struct LeafPageHandler {}

// Returned from adding a tuple to a leaf page.
pub struct UpdateResult {
    // The set of leaf pages after adding a tuple to a leaf page.
    // If the page did not split when adding the tuple then there
    // should only be one page in the vec. There can be up to
    // 3 pages in the vec, this can happen if the tuples are large
    // and caused an uneven split.
    pub tree_leaf_pages: Vec<(LeafPage, Option<Vec<u8>>)>,
    // Any tuple that was deleted as a result of adding another tuple.
    pub deleted_tuple: Option<Tuple>,
}

impl LeafPageHandler {
    // This is called after after overflow handling, so we know the tuple is small enough
    // to fit in a page. However, the page may be too full to hold the tuple in which case
    // the page will need to split. There may be some large items in the page so it
    // may need to be split into three pages to actually accomodate the tuple.
    //
    // Adding this tuple may remove an existing tuple in the page, and this function
    // will return this tuple in the UpdateResult.
    //
    // The function will also return the page/pages after the add operation. There
    // can be one page (ie the tuple was successfully added to the original page) or
    // 2 or 3 pages returned (ie the original page was split into up to three pages).
    // The pages are returned as
    //
    //     Vec<(LeafPage, Option<Vec<u8>>)>
    //
    // The Option<Vec<u8>> is the left most key of the page if the page is a new
    // page as a result of a split. This left most key is then pushed up into the parent
    // directory node as the reference to the page.
    // The original page will have this as None - the caller can then know which page in
    // in the vec refers to the original page, it uses this knowledge when updating the
    // parent directory for the pages.
    //
    // This method is a wrapper around add_to_leaf_page.
    pub fn add_tuple(page: LeafPage, tuple: Tuple) -> UpdateResult {
        let mut pages: Vec<(LeafPage, Option<Vec<u8>>)> = Vec::new();
        pages.push((page, None));
        // Pass in the page as an entry in a list, this allows the function
        // LeafPageHandler::add_to_leaf_page to be called recursively if
        // pages need to be split.
        let deleted_tuple = LeafPageHandler::add_to_leaf_page(tuple, &mut pages);
        UpdateResult {
            tree_leaf_pages: pages,
            deleted_tuple,
        }
    }

    // Add a tuple to a leaf page, the leaf page is the last entry
    // in the leaf_page_stack. A stack is used in case pages split
    // and this function has to be called recursively.
    //
    // The first time this function is called for a page/tuple there
    // will only be one leaf page in the stack. If this function
    // is called recursively then the tuple cannot be added even
    // after a page split, it requires another split (the original
    // page as split into three parts) - this supports an edge case
    // when tuples can mostly fill pages. Its a lot of complexity
    // for an edge case that could be addressed by being more
    // restrictive with tuple sizes.
    //
    // If a tuple is replaced then the original tuple is returned.
    fn add_to_leaf_page(
        tuple: Tuple,
        leaf_page_stack: &mut Vec<(LeafPage, Option<Vec<u8>>)>,
    ) -> Option<Tuple> {
        assert!(!leaf_page_stack.is_empty());
        let (page, page_left_key) = leaf_page_stack.last_mut().unwrap();

        // If the tuple cannot be added to the page because it is too
        // small then ok will be false, however any existing tuple for
        // the key will be removed and returned here.
        let (ok, existing_tuple) = page.add_tuple(&tuple);
        if ok {
            // Tuple was added without needing to split the page.
            return existing_tuple;
        }

        // Tuple was not added, so we need to split the page and add it to the correct page.
        // The optional_left_key is the left most key of the right_page, if the right_page
        // is empty then it will be None.
        // Pass in a version of '0' - this will be overwritten later.
        let (mut left_page, mut right_page, optional_left_key) = page.split_page(0);

        // The original page is replaced with the left page, this will have the
        // left most key of the original page. On first call this left most key will be None
        // but if called recursively it may not so we take a copy here.
        let copy_page_left_key = if page_left_key.is_none() {
            None
        } else {
            Some(page_left_key.as_ref().unwrap().to_vec())
        };
        // Remove the original page from the stack. The pages from the split
        // will added to the stack.
        leaf_page_stack.pop();

        // Edge case, the original page had only one entry. The new right page
        // is empty so add tuple to it. We can assume it can fit into a page.
        // In this case the optional_left_key will be None.
        if right_page.is_empty() {
            let (ok, _) = right_page.add_tuple(&tuple);
            assert!(ok);
            // Push the two pages back on the stack to be written back.
            leaf_page_stack.push((left_page, copy_page_left_key));
            leaf_page_stack.push((right_page, Some(tuple.get_key().to_vec())));
            return existing_tuple;
        }

        // optional_left_key should not be None now.
        assert!(optional_left_key.is_some());
        let left_key = optional_left_key.unwrap();

        // Tuple is to the left of the slit key so try and add to the left page.
        if tuple.get_key() < left_key.as_slice() {
            let (ok, _) = left_page.add_tuple(&tuple);
            if ok {
                // Order does not matter as pages won't be split.
                leaf_page_stack.push((left_page, copy_page_left_key));
                leaf_page_stack.push((right_page, Some(left_key)));
                existing_tuple
            } else {
                // Tuple does not fit into the left page, need to split again,
                // Put the right page back on the stack first, then the left page -
                // as the last page it will be split again.
                leaf_page_stack.push((right_page, Some(left_key)));
                leaf_page_stack.push((left_page, copy_page_left_key));
                let empty_tuple = LeafPageHandler::add_to_leaf_page(tuple, leaf_page_stack);
                assert!(
                    empty_tuple.is_none(),
                    "Second call to add tuple should not delete tuple"
                );
                existing_tuple
            }
        } else {
            // If we get here then the tuple should go into the right page if it can fit.
            let (ok, _) = right_page.add_tuple(&tuple);
            if ok {
                leaf_page_stack.push((left_page, copy_page_left_key));
                leaf_page_stack.push((right_page, Some(left_key)));
                existing_tuple
            } else {
                // Tuple cannot fit into right page. Put the left page into
                // the stack first and then the right page, as the last page
                // it will be split again.
                leaf_page_stack.push((left_page, copy_page_left_key));
                leaf_page_stack.push((right_page, Some(left_key)));
                let empty_tuple = LeafPageHandler::add_to_leaf_page(tuple, leaf_page_stack);
                assert!(
                    empty_tuple.is_none(),
                    "Second call to add tuple should not delete tuple"
                );
                existing_tuple
            }
        }
    }

    // Loop through the set of leaf pages references. If the page
    // number in the page is zero then its a new page, we
    // need to get a new page number for it from the free_page_tracker
    // and set the version.
    // If the page number for the page is not zero, then its an
    // existing page. The old page number needs to be returned so
    // it can be used again and the we need to get the page a new
    // page number.
    pub fn map_pages(
        leaf_page_refs: &mut Vec<(LeafPage, Option<Vec<u8>>)>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        version: u64,
    ) {
        for (page, _) in leaf_page_refs {
            let old_page_no = page.get_page_number();
            if old_page_no.to_u64() != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            page.set_page_number(new_page_no);
            page.set_version(version);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_layer::DbConfig;
    use crate::page_no::PageNo;

    #[test]
    fn test_small_tuple_split_page() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let version = 0;

        let mut tree_leaf_page: LeafPage =
            LeafPage::create_new(&page_config, PageNo::new(0, 56), version + 1);
        let mut new_version = version;
        let mut tuple: Tuple;
        let mut pages: UpdateResult = UpdateResult {
            tree_leaf_pages: Vec::new(),
            deleted_tuple: None,
        };
        // Each loop is a new commit.
        for i in 1u32..1024 {
            new_version += 1;
            tuple = Tuple::new(
                i.to_le_bytes().to_vec().as_ref(),
                i.to_le_bytes().to_vec().as_ref(),
                new_version,
            );

            pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            if pages.tree_leaf_pages.len() > 1 {
                break;
            }
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }

        assert!(pages.tree_leaf_pages.len() == 2);
        let tree_leaf_page1 = pages.tree_leaf_pages.pop().unwrap().0;
        assert_eq!(tree_leaf_page1.get_no_page_entries(), 98);
        let tree_leaf_page2 = pages.tree_leaf_pages.pop().unwrap().0;
        assert_eq!(tree_leaf_page2.get_no_page_entries(), 96);
    }

    // In this test we overwrite the same key twice.
    #[test]
    fn test_over_write_big_tuples() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, PageNo::new(0, 0), 1);
        let mut new_version = 2;
        let mut tuple: Tuple;
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for _i in 0u32..2 {
            new_version += 1;
            let value = vec![0u8; 2048];
            // Same key used
            tuple = Tuple::new(
                1u32.to_le_bytes().to_vec().as_ref(),
                value.as_ref(),
                new_version,
            );

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }
        assert_eq!(tree_leaf_page_count, 1);
    }

    #[test]
    fn test_add_big_tuples() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, PageNo::new(0, 0), 0);
        tree_leaf_page.set_version(version + 1);
        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..2 {
            new_version += 1;
            let value = vec![0u8; 2048];
            tuple = Tuple::new(
                i.to_le_bytes().to_vec().as_ref(),
                value.as_ref(),
                new_version,
            );

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }
        assert_eq!(tree_leaf_page_count, 2);
    }

    #[test]
    fn test_add_small_large_large() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, PageNo::new(0, 0), 0);
        tree_leaf_page.set_version(version + 1);

        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version += 1;
            let value: Vec<u8> = if i == 0 {
                vec![0u8; 8]
            } else {
                vec![0u8; 2048]
            };
            // Use the same key
            tuple = Tuple::new(
                i.to_le_bytes().to_vec().as_ref(),
                value.as_ref(),
                new_version,
            );
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }
        assert_eq!(tree_leaf_page_count, 3);
    }

    #[test]
    fn test_add_large_small_large() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, PageNo::new(0, 0), 0);
        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version += 1;
            let value: Vec<u8> = if i == 1 {
                vec![0u8; 8]
            } else {
                vec![0u8; 2048]
            };
            // Use the same key
            tuple = Tuple::new(
                i.to_le_bytes().to_vec().as_ref(),
                value.as_ref(),
                new_version,
            );

            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }
        assert_eq!(tree_leaf_page_count, 2);
    }

    #[test]
    fn test_add_small_large_large_reverse() {
        let page_config = DbConfig {
            block_size: 4096,
            page_size: 4092,
            block_sanity_size: 4,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, PageNo::new(0, 0), 0);
        tree_leaf_page.set_version(version + 1);

        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version += 1;
            let j = 3 - i;
            let value: Vec<u8> = if j == 3 {
                vec![0u8; 8]
            } else {
                vec![0u8; 2048]
            };
            // Use the same key
            tuple = Tuple::new(
                j.to_le_bytes().to_vec().as_ref(),
                value.as_ref(),
                new_version,
            );
            let mut pages = LeafPageHandler::add_tuple(tree_leaf_page, tuple);
            tree_leaf_page_count = pages.tree_leaf_pages.len();
            tree_leaf_page = pages.tree_leaf_pages.pop().unwrap().0;
        }
        assert_eq!(tree_leaf_page_count, 3);
    }
}

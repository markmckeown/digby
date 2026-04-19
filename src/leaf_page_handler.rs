use crate::free_page_tracker::FreePageTracker;
use crate::leaf_page::LeafPage;
use crate::page::PageTrait;
use crate::page_cache::PageCache;
use crate::tuple::{Tuple, TupleTrait};

pub struct LeafPageHandler {}

pub struct UpdateResult {
    pub tree_leaf_pages: Vec<(LeafPage, Option<Vec<u8>>)>,
    pub deleted_tuple: Option<Tuple>,
}

impl LeafPageHandler {
    // This happens after overflow handling, so we know the tuple will fit in a page.
    pub fn add_tuple(page: LeafPage, tuple: Tuple) -> UpdateResult {
        let mut pages: Vec<(LeafPage, Option<Vec<u8>>)> = Vec::new();
        pages.push((page, None));
        let deleted_tuple = LeafPageHandler::add_to_leaf_page(tuple, &mut pages);
        UpdateResult {
            tree_leaf_pages: pages,
            deleted_tuple,
        }
    }

    pub fn map_pages(
        pages: &mut Vec<(LeafPage, Option<Vec<u8>>)>,
        free_page_tracker: &mut FreePageTracker,
        page_cache: &mut PageCache,
        version: u64,
    ) {
        for entry in pages {
            let old_page_no = entry.0.get_page_number();
            if old_page_no != 0 {
                free_page_tracker.return_free_page_no(old_page_no);
            }
            let new_page_no = free_page_tracker.get_free_page(page_cache);
            entry.0.set_page_number(new_page_no);
            entry.0.set_version(version);
        }
    }

    fn add_to_leaf_page(
        tuple: Tuple,
        new_pages: &mut Vec<(LeafPage, Option<Vec<u8>>)>,
    ) -> Option<Tuple> {
        assert!(!new_pages.is_empty());
        let page = new_pages.last_mut().unwrap();
        
        let (ok, existing_tuple) = page.0.add_tuple(&tuple);
        if ok {
            // Tuple was added without needing to split the page.
            return existing_tuple;
        }

        // Tuple was not added, so we need to split the page and add it to the correct page.
        // TODO - handle split when no entries go into right page.
        let (mut left_page, mut right_page, opton_left_key) = page.0.split_page(0);
        new_pages.pop();

        //assert!(!right_page.is_empty(), "Right page is empty after split");
        //assert!(!left_page.is_empty(), "Left page is empty after split");
        if right_page.is_empty() {
            // Edge case, the page cannot be split as it has only one entry. The new page
            // is empty so add tuple to it. We can assume it can fit into a page.
            let (ok, _) = right_page.add_tuple(&tuple);
            assert!(ok);
            new_pages.push((left_page, None));
            new_pages.push((right_page, Some(tuple.get_key().to_vec())));
            return existing_tuple;
        }

        let left_key = opton_left_key.unwrap();

        // Tuple is to the left of the split entries so try and add to the left page.
        if tuple.get_key() < left_key.as_slice() {
            let (ok, _) = left_page.add_tuple(&tuple);
            if ok {
                new_pages.push((left_page, None));
                new_pages.push((right_page, Some(left_key)));
                existing_tuple
            } else {
                // Tuple does not fit into the old page, need to split again,
                // Put the new page to the start of the new_pages - the old
                // page is still the last entry adn will be split again when
                // recursively called.
                new_pages.push((right_page, Some(left_key)));
                new_pages.push((left_page, None));
                let empty_tuple = LeafPageHandler::add_to_leaf_page(tuple, new_pages);
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
                new_pages.push((left_page, None));
                new_pages.push((right_page, Some(left_key)));
                existing_tuple
            } else {
                // Tuple cannot fit into new page, new page is the last in the list
                // and will be split again when function is recursively called.
                new_pages.push((left_page, None));
                new_pages.push((right_page, Some(left_key)));
                let empty_tuple = LeafPageHandler::add_to_leaf_page(tuple, new_pages);
                assert!(
                    empty_tuple.is_none(),
                    "Second call to add tuple should not delete tuple"
                );
                existing_tuple
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_layer::PageConfig;

    #[test]
    fn test_small_tuple_split_page() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let version = 0;

        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 56, version + 1);
        let mut new_version = version;
        let mut tuple: Tuple;
        let mut pages: UpdateResult = UpdateResult {
            tree_leaf_pages: Vec::new(),
            deleted_tuple: None,
        };
        // Each loop is a new commit.
        for i in 1u32..1024 {
            new_version = new_version + 1;
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
        assert_eq!(tree_leaf_page1.get_entries_size(), 98);
        let tree_leaf_page2 = pages.tree_leaf_pages.pop().unwrap().0;
        assert_eq!(tree_leaf_page2.get_entries_size(), 96);
    }

    // In this test we overwrite the same key twice.
    #[test]
    fn test_over_write_big_tuples() {
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 0, 1);
        let mut new_version = 2;
        let mut tuple: Tuple;
        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for _i in 0u32..2 {
            new_version = new_version + 1;
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
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 0, 0);
        tree_leaf_page.set_version(version + 1);
        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..2 {
            new_version = new_version + 1;
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
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 0, 0);
        tree_leaf_page.set_version(version + 1);

        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let value: Vec<u8>;
            if i == 0 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
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
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 0, 0);
        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let value: Vec<u8>;
            if i == 1 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
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
        let page_config = PageConfig {
            block_size: 4096,
            page_size: 4092,
        };
        let version = 0;
        let mut tree_leaf_page: LeafPage = LeafPage::create_new(&page_config, 0, 0);
        tree_leaf_page.set_version(version + 1);

        let mut new_version = version;
        let mut tuple: Tuple;

        // Each loop is a new commit.
        let mut tree_leaf_page_count = 0;
        for i in 0u32..3 {
            new_version = new_version + 1;
            let j = 3 - i;
            let value: Vec<u8>;
            if j == 3 {
                value = vec![0u8; 8];
            } else {
                value = vec![0u8; 2048];
            }
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

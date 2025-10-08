use crate::page::PageTrait;
use crate::{FreePageTracker, TreeDirEntry, TreeInternalPage, TreeLeafPage};
use crate::page_cache::PageCache;

pub struct TreeDirHandler {

}

impl TreeDirHandler {
    pub fn handle_tree_leaf_store(
            mut tree_dir_page: TreeInternalPage, 
            mut leaf_pages: Vec<TreeLeafPage>,
            page_size: usize) -> Vec<TreeInternalPage> {
        assert!(!leaf_pages.is_empty(), "leaf_pages was empty");
        let mut tree_dir_pages: Vec<TreeInternalPage> = Vec::new();

        // Sort the leaf pages and build a set of TreeDirEntry.
        leaf_pages.sort_by(|b, a| b.get_left_key(page_size).unwrap().cmp(&a.get_left_key(page_size).unwrap()));
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in leaf_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

        // Do not need to split the tree dir page.
        if tree_dir_page.can_fit_entries(&entries) {
            tree_dir_page.add_entries(entries, page_size);
            tree_dir_pages.push(tree_dir_page);
            return tree_dir_pages;
        }

        // Need to split the tree dir page.
        let entries_to_right = tree_dir_page.get_right_half_entries(page_size);
        assert!(!entries_to_right.is_empty());
        let use_original_page: bool = entries.get(0).unwrap().get_key() < entries_to_right.get(0).unwrap().get_key();
        let mut new_tree_dir_page = TreeInternalPage::new(page_size as u64, 0, 0);
        new_tree_dir_page.set_parent_page(tree_dir_page.get_parent_page());
        new_tree_dir_page.set_page_to_left(entries_to_right.get(0).unwrap().get_page_no());
        // Do NOT iterate from 0, we skip first dir entry as we added it as the page to the left
        for i in 1..entries_to_right.len() {
           new_tree_dir_page.add_tree_dir_entry(entries_to_right.get(i).unwrap(), page_size as u64);
        } 

        if use_original_page {
            tree_dir_page.add_entries(entries, page_size);
        } else {
            new_tree_dir_page.add_entries(entries, page_size);
        }
        tree_dir_pages.push(tree_dir_page);
        tree_dir_pages.push(new_tree_dir_page);

        return tree_dir_pages;
    }


    pub fn map_pages(pages: &mut Vec<TreeInternalPage>, 
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


    pub fn handle_tree_dir_store(
            mut parent_dir_page: TreeInternalPage, 
            mut dir_pages: Vec<TreeInternalPage>, 
            free_page_tracker: &mut FreePageTracker,
            page_cache: &mut PageCache,
            version: u64,
            page_size: usize) -> Vec<TreeInternalPage> {
        assert!(!dir_pages.is_empty(), "leaf_pages was empty");
        let mut tree_dir_pages: Vec<TreeInternalPage> = Vec::new();

        // Sort the leaf pages and build a set of TreeDirEntry.
        dir_pages.sort_by(|b, a| b.get_dir_left_key(page_size).unwrap().cmp(&a.get_dir_left_key(page_size).unwrap()));
        let mut entries: Vec<TreeDirEntry> = Vec::new();
        for leaf_page in dir_pages {
            let tree_dir_entry = TreeDirEntry::new(leaf_page.get_dir_left_key(page_size).unwrap(), leaf_page.get_page_number());
            entries.push(tree_dir_entry);
        }

        // Do not need to split the tree dir page.
        if parent_dir_page.can_fit_entries(&entries) {
            parent_dir_page.add_entries(entries, page_size);
            tree_dir_pages.push(parent_dir_page);
            return tree_dir_pages;
        }

        // Need to split the parent dir page.
        let entries_to_right = parent_dir_page.get_right_half_entries(page_size);
        assert!(!entries_to_right.is_empty());
        let use_original_page: bool = entries.get(0).unwrap().get_key() < entries_to_right.get(0).unwrap().get_key();
        let mut new_tree_page = TreeInternalPage::new(page_size as u64, 0, version);
        new_tree_page.set_parent_page(parent_dir_page.get_parent_page());
        new_tree_page.set_page_to_left(entries_to_right.get(0).unwrap().get_page_no());
        // Do NOT iterate from 0, we skip first dir entry as we added it as the page to the left
        for i in 1..entries_to_right.len() {
           new_tree_page.add_tree_dir_entry(entries_to_right.get(i).unwrap(), page_size as u64);
        } 

        if use_original_page {
            parent_dir_page.add_entries(entries, page_size);
        } else {
            new_tree_page.add_entries(entries, page_size);
        }
        tree_dir_pages.push(parent_dir_page);
        tree_dir_pages.push(new_tree_page);


        TreeDirHandler::map_pages(&mut tree_dir_pages, free_page_tracker, page_cache, version);
        return tree_dir_pages;
    }

}


#[cfg(test)]
mod tests {
    use crate::Tuple;

    use super::*;

    // Fill in the left_most_page
    #[test]
    fn test_1() {
        let page_size: usize = 4096;
        let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        tree_leaf_page.set_page_number(21);
        let tuple: Tuple = Tuple::new(b"a".to_vec(), b"a_value".to_vec(), 345);
        tree_leaf_page.store_tuple(tuple, page_size);
        let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
        leaf_pages.push(tree_leaf_page);

        let mut tree_dir_page = TreeInternalPage::new(page_size as u64, 0, 0);
        tree_dir_page.add_page_entry(23, b"m".to_vec(), 79, page_size);

        let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, leaf_pages, page_size);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap();
        assert_eq!(tree_dir_page.get_page_to_left(), 21);
        assert_eq!(tree_dir_page.get_dir_left_key(page_size).unwrap(), b"m".to_vec());
    }

    // Replace a key
    #[test]
    fn test_2() {
        let page_size: usize = 4096;
        let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        let tuple: Tuple = Tuple::new(b"a".to_vec(), b"a_value".to_vec(), 345);
        tree_leaf_page.set_page_number(21);
        tree_leaf_page.store_tuple(tuple, page_size);
        let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
        leaf_pages.push(tree_leaf_page);

        let mut tree_dir_page = TreeInternalPage::new(page_size as u64, 0, 0);
        tree_dir_page.add_page_entry(23, b"a".to_vec(), 79, page_size);
        assert_eq!(tree_dir_page.get_next_page(b"a".to_vec(), page_size), 79);

        let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, leaf_pages, page_size);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap();
        assert_eq!(tree_dir_page.get_page_to_left(), 23);
        assert_eq!(tree_dir_page.get_dir_left_key(page_size).unwrap(), b"a".to_vec());
        // Page for key a is now 21 rather than 79
        assert_eq!(tree_dir_page.get_next_page(b"a".to_vec(), page_size), 21);
        assert_eq!(tree_dir_page.get_next_page(b"b".to_vec(), page_size), 21);
        assert_eq!(tree_dir_page.get_next_page(b"0".to_vec(), page_size), 23);
    }

    // Key goes to the right
    #[test]
    fn test_3() {
        let page_size: usize = 4096;
        let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        let tuple: Tuple = Tuple::new(b"m".to_vec(), b"m_value".to_vec(), 345);
        tree_leaf_page.set_page_number(99);
        tree_leaf_page.store_tuple(tuple, page_size);
        let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
        leaf_pages.push(tree_leaf_page);

        let mut tree_dir_page = TreeInternalPage::new(page_size as u64, 0, 0);
        tree_dir_page.add_page_entry(23, b"a".to_vec(), 79, page_size);

        let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, leaf_pages, page_size);
        assert_eq!(new_pages.len(), 1);
        tree_dir_page = new_pages.pop().unwrap();
        assert_eq!(tree_dir_page.get_page_to_left(), 23);
        assert_eq!(tree_dir_page.get_dir_left_key(page_size).unwrap(), b"a".to_vec());
        assert_eq!(tree_dir_page.get_next_page(b"m".to_vec(), page_size), 99);
        assert_eq!(tree_dir_page.get_next_page(b"s".to_vec(), page_size), 99);
    }


    // Fill Dir Page
    #[test]
    fn test_4() {
        let page_size: usize = 4096;
        
        let mut tree_dir_page = TreeInternalPage::new(page_size as u64, 0, 0);
        tree_dir_page.add_page_entry(23, 0u32.to_le_bytes().to_vec(), 79, page_size);
        let mut j: u32 = 0;
        for i in 1u32 .. 4000 {
            j = i;
            let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
            tree_leaf_page.set_page_number(i);
            let tuple: Tuple = Tuple::new(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec(), 345);
            tree_leaf_page.store_tuple(tuple, page_size);
            let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
            leaf_pages.push(tree_leaf_page);
            if !tree_dir_page.can_fit(TreeDirEntry::new(i.to_be_bytes().to_vec(), i).get_byte_size()) {
                break;
            }
            let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, leaf_pages, page_size);
            assert_eq!(new_pages.len(), 1);
            tree_dir_page = new_pages.pop().unwrap();
        }
        assert_eq!(tree_dir_page.get_entries(), 339 as u16);

        let mut tree_leaf_page = TreeLeafPage::new(page_size as u64, 0);
        tree_leaf_page.set_page_number(j);
        let tuple: Tuple = Tuple::new(j.to_be_bytes().to_vec(), j.to_be_bytes().to_vec(), 345);
        tree_leaf_page.store_tuple(tuple, page_size);
        let mut leaf_pages: Vec<TreeLeafPage> = Vec::new();
        leaf_pages.push(tree_leaf_page);
        let mut new_pages = TreeDirHandler::handle_tree_leaf_store(tree_dir_page, leaf_pages, page_size);
        assert_eq!(new_pages.len(), 2);
        let first = new_pages.pop().unwrap();
        assert_eq!(first.get_entries(), 169 as u16);
        assert_eq!(first.get_page_to_left(), 170);
        
        let second = new_pages.pop().unwrap();
        assert_eq!(second.get_entries(), 170 as u16);
        assert_eq!(second.get_page_to_left(), 23);

    }

}